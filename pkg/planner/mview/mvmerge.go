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

package mvmerge

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table/tables"
)

const (
	deltaTableAlias      = "delta"
	mvTableAlias         = "mv"
	fullUpdateOuterAlias = "full_outer"
	fullUpdateInnerAlias = "full_inner"

	deltaCntStarName = "__mvmerge_delta_cnt_star"
	mvRowIDName      = "__mvmerge_mv_rowid"
)

// SQL construction overview:
//   1) buildLocal validates MV/base/mlog metadata, parses MV SQL, and extracts layout metadata.
//   2) buildMLogDeltaSelect builds stage-1 aggregation on mlog rows inside (FromTS, ToTS].
//   3) buildMergeSourceSelect LEFT JOINs stage-1 deltas with current MV rows to produce a fixed output schema:
//      [all delta payload columns][all MV columns][optional rowid handle].
// The executor consumes this one merged source stream and applies aggregate-specific update rules by offset.

// BuildOptions defines the commit-ts lower bound (FromTS, +inf) used to read incremental mv-log rows.
// Upper bound is provided by statement snapshot ts (for_update_ts at execution time).
type BuildOptions struct {
	FromTS uint64
}

// BuildResult is the merge source produced by Build.
// It includes the merge-source SELECT statement and all metadata needed by executor-side MV merge.
// Row layout of MergeSourceSelect output is fixed:
//  1. delta columns first
//  2. MV output columns after delta columns (count = MVColumnCount)
//  3. optional _tidb_rowid handle column at the end when MV table uses extra row-id handle
//
// All offsets in AggInfos (MVOffset and Dependencies) are based on this layout.
type BuildResult struct {
	MergeSourceSelect *ast.SelectStmt
	// SourceColumnCount is the expected number of output columns of MergeSourceSelect after optimization.
	SourceColumnCount int
	// FullUpdateLookupTemplateSelect is an optional index-lookup template used for group-level full recomputation.
	// It is generated when MV contains MIN/MAX and is expected to optimize to an IndexJoin-style plan where:
	//   - the inner child outputs fallback recomputation columns (group keys + MIN/MAX-related aggregates)
	//   - the outer child only provides group-key probe values
	FullUpdateLookupTemplateSelect *ast.SelectStmt
	// FullUpdateLookupColumnCount is the expected output column count of FullUpdateLookupTemplateSelect.
	FullUpdateLookupColumnCount int
	// FullUpdateLookupMVOffsets maps full-update lookup output columns to MV output offsets.
	// Its length equals FullUpdateLookupColumnCount when FullUpdateLookupTemplateSelect is not nil.
	FullUpdateLookupMVOffsets []int

	MVTableID   int64
	BaseTableID int64
	MLogTableID int64

	// MVColumnCount indicates how many columns in the merge-source output schema belong to the MV row shape.
	// The MV columns are always put after all delta columns.
	MVColumnCount int
	// DeltaColumnCount indicates how many columns in the merge-source output schema belong to delta payload.
	// Delta columns are always at the beginning of merge-source output schema.
	DeltaColumnCount int

	// MVTablePKCols stores MV table handle columns with their positions in merge-source output schema.
	// For extra row-id handle, it points to the trailing _tidb_rowid column.
	MVTablePKCols plannerutil.HandleCols

	// GroupKeyMVOffsets are offsets (0-based) of the group key columns in the MV output schema.
	GroupKeyMVOffsets []int

	// CountStarMVOffset is the offset (0-based) of COUNT(*) in MV output columns.
	// Build returns error when MV definition does not include COUNT(*).
	CountStarMVOffset int

	AggInfos []AggInfo
}

// DeltaColumn describes one delta payload column in merge-source output.
type DeltaColumn struct {
	Name   string
	Offset int
}

// AggKind classifies supported aggregate functions in MV merge.
type AggKind int

const (
	// AggCountStar represents COUNT(*).
	AggCountStar AggKind = iota
	// AggCount represents COUNT(expr).
	AggCount
	// AggSum represents SUM(expr).
	AggSum
	// AggMin represents MIN(expr).
	AggMin
	// AggMax represents MAX(expr).
	AggMax
)

// AggInfo describes one aggregate output column and dependency offsets used in update order.
type AggInfo struct {
	Kind AggKind

	// MVOffset is the offset (0-based) in the MV output schema.
	MVOffset int

	// ArgColName is the base-table column name used as the aggregate argument. Empty for COUNT(*).
	ArgColName string

	// Dependencies stores dependency offsets in merge-source output schema.
	// The meaning depends on Kind:
	//   - AggCountStar / AggCount: [self_delta]
	//   - AggSum:
	//     - [self_delta] when the SUM argument is NOT NULL (inferred from base table schema or passed by caller),
	//       no COUNT(expr) dependency needed.
	//     - [self_delta, matched_count_expr_mv] otherwise.
	//     - self_delta: offset of SUM(expr) delta column.
	//     - matched_count_expr_mv: absolute output offset of matched COUNT(expr) MV column.
	//       COUNT(expr) must be updated before SUM(expr), and SUM should read this updated MV value.
	//       The same matched COUNT(expr) can be a dependency for multiple aggregate functions.
	//   - AggMax / AggMin:
	//     - [added_val, added_cnt, removed_val, removed_cnt] when argument is NOT NULL.
	//     - [added_val, added_cnt, removed_val, removed_cnt, matched_count_expr_mv] otherwise.
	//     - added_cnt/removed_cnt are counts of rows whose argument equals added_val/removed_val
	//       in the added/removed subdomain respectively (MAX/MIN_COUNT semantics).
	Dependencies []int
}

type aggColInfo struct {
	info                AggInfo
	deltaName           string
	addedCountDeltaName string
	removedValueDelta   string
	removedCountDelta   string
	argExpr             ast.ExprNode
}

// buildLocalResult contains the parsed MV definition and all metadata derived from it.
// It is used to decouple MV-definition parsing/analysis from building the merge-source SQL.
//
// The caller may further build/optimize the MV definition logical plan based on MVSelect.
// After that, buildFromLocal can be used to construct the final merge-source SELECT.
type buildLocalResult struct {
	// MVSelect is the parsed MV definition SELECT statement.
	// Note: buildFromLocal may mutate parts of this AST (e.g. strip column qualifiers in WHERE).
	MVSelect *ast.SelectStmt
	sctx     planctx.PlanContext

	mvDBName     pmodel.CIStr
	mv           *model.TableInfo
	baseTableID  int64
	mlogTableID  int64
	baseTable    *model.TableInfo
	mlogTable    *model.TableInfo
	groupKeySet  map[int]struct{}
	groupKeyOffs []int
	aggCols      []aggColInfo
	hasMinMax    bool

	countStarMVOffset int
}

// Build constructs the merge-source plan and metadata for one MV incremental merge window.
// Internally it runs two stages: buildLocal metadata derivation and buildFromLocal SQL assembly.
func Build(
	sctx planctx.PlanContext,
	is infoschema.InfoSchema,
	mv *model.TableInfo,
	opt BuildOptions,
	aggArgNotNullByOffset map[int]bool,
) (*BuildResult, error) {
	local, err := buildLocal(sctx, is, mv)
	if err != nil {
		return nil, err
	}
	return buildFromLocal(local, opt, aggArgNotNullByOffset)
}

// buildLocal validates MV/MLoG metadata, parses the MV definition, and derives local layout metadata.
func buildLocal(
	sctx planctx.PlanContext,
	is infoschema.InfoSchema,
	mv *model.TableInfo,
) (*buildLocalResult, error) {
	// Stage 0: validate MV/MLoG metadata and locate all required tables.
	if mv == nil {
		return nil, errors.New("mv table info is nil")
	}
	if mv.MaterializedView == nil {
		return nil, errors.Errorf("table %s is not a materialized view", mv.Name.O)
	}
	if len(mv.MaterializedView.BaseTableIDs) != 1 {
		return nil, errors.Errorf(
			"materialized view %s has invalid base table list size %d",
			mv.Name.O,
			len(mv.MaterializedView.BaseTableIDs),
		)
	}
	baseTableID := mv.MaterializedView.BaseTableIDs[0]
	baseTable, ok := is.TableInfoByID(baseTableID)
	if !ok {
		return nil, errors.Errorf("base table id %d not found in infoschema", baseTableID)
	}
	if baseTable.MaterializedViewBase == nil || baseTable.MaterializedViewBase.MLogID == 0 {
		return nil, errors.Errorf("base table %s has no materialized view log", baseTable.Name.O)
	}
	mlogTableID := baseTable.MaterializedViewBase.MLogID
	mlogTable, ok := is.TableInfoByID(mlogTableID)
	if !ok {
		return nil, errors.Errorf("materialized view log table id %d not found in infoschema", mlogTableID)
	}
	if mlogTable.MaterializedViewLog == nil {
		return nil, errors.Errorf("table %s is not a materialized view log", mlogTable.Name.O)
	}
	if mlogTable.MaterializedViewLog.BaseTableID != baseTableID {
		return nil, errors.Errorf(
			"materialized view log %s does not belong to base table id %d",
			mlogTable.Name.O,
			baseTableID,
		)
	}
	if !hasColumn(mlogTable, model.MaterializedViewLogOldNewColumnName) {
		return nil, errors.Errorf(
			"materialized view log %s missing required column %s",
			mlogTable.Name.O,
			model.MaterializedViewLogOldNewColumnName,
		)
	}
	if !hasColumn(mlogTable, model.MaterializedViewLogDMLTypeColumnName) {
		return nil, errors.Errorf(
			"materialized view log %s missing required column %s",
			mlogTable.Name.O,
			model.MaterializedViewLogDMLTypeColumnName,
		)
	}

	// Stage 1: parse MV definition and derive merge key/aggregate layout from it.
	mvDBName, err := dbNameByTableID(is, mv.ID)
	if err != nil {
		return nil, err
	}

	mvSel, err := parseSelectFromSQL(sctx, mv.MaterializedView.SQLContent)
	if err != nil {
		return nil, err
	}

	groupKeyOffsets, err := extractGroupKeyOffsetsFromMVSelect(mvSel)
	if err != nil {
		return nil, err
	}
	if len(groupKeyOffsets) == 0 {
		return nil, errors.New("materialized view definition has empty GROUP BY")
	}
	// Fast membership check when deciding whether an MV output column is a group key.
	groupKeySet := make(map[int]struct{}, len(groupKeyOffsets))
	for _, off := range groupKeyOffsets {
		groupKeySet[off] = struct{}{}
	}

	aggCols, hasMinMax, err := extractAggInfosFromMVSelect(mvSel)
	if err != nil {
		return nil, err
	}

	if len(mv.Columns) != len(mvSel.Fields.Fields) {
		// This should never happen for valid MV metadata. Keep it as a guard to avoid mismatched join schema.
		return nil, errors.Errorf(
			"mv columns count %d does not match mv query output %d",
			len(mv.Columns),
			len(mvSel.Fields.Fields),
		)
	}

	countStarMVOffset := -1
	for _, ac := range aggCols {
		if ac.info.Kind == AggCountStar {
			countStarMVOffset = ac.info.MVOffset
			break
		}
	}
	if countStarMVOffset < 0 {
		return nil, errors.New("materialized view definition must include COUNT(*) for mvmerge")
	}

	return &buildLocalResult{
		MVSelect:          mvSel,
		sctx:              sctx,
		mvDBName:          mvDBName,
		mv:                mv,
		baseTableID:       baseTableID,
		mlogTableID:       mlogTableID,
		baseTable:         baseTable,
		mlogTable:         mlogTable,
		groupKeySet:       groupKeySet,
		groupKeyOffs:      groupKeyOffsets,
		aggCols:           aggCols,
		hasMinMax:         hasMinMax,
		countStarMVOffset: countStarMVOffset,
	}, nil
}

// buildFromLocal constructs merge-source SQL and dependency metadata from buildLocalResult.
//
// Final SQL shape:
//
//	SELECT <delta payload cols>, <mv cols>, [mv._tidb_rowid AS __mvmerge_mv_rowid]
//	FROM (
//	  <stage-1 delta aggregation on mlog>
//	) AS delta
//	LEFT JOIN <mv table> AS mv
//	  ON delta.<group_key_1> <=> mv.<group_key_1>
//	 AND ...
//
// Stage-1 delta columns are always contiguous at the beginning of output schema, so aggregate
// dependency offsets remain stable and can be consumed directly by executor logic.
func buildFromLocal(
	local *buildLocalResult,
	opt BuildOptions,
	aggArgNotNullByOffset map[int]bool,
) (*BuildResult, error) {
	if local == nil {
		return nil, errors.New("mvmerge: local result is nil")
	}
	if local.MVSelect == nil {
		return nil, errors.New("mvmerge: local MVSelect is nil")
	}
	if aggArgNotNullByOffset == nil {
		aggArgNotNullByOffset = inferAggArgNotNullByOffset(local)
	}

	// Stage 2: build merge source SQL in two steps:
	//   1) aggregate mlog rows into per-group deltas
	//   2) left join those deltas with current MV snapshot
	deltaSel, err := buildMLogDeltaSelect(
		local.sctx,
		local.mvDBName,
		local.mlogTable,
		local.MVSelect,
		local.mv.Columns,
		local.groupKeyOffs,
		local.aggCols,
		opt,
	)
	if err != nil {
		return nil, err
	}
	var fullUpdateSel *ast.SelectStmt
	fullUpdateColumnCount := 0
	var fullUpdateMVOffsets []int
	if local.hasMinMax {
		fullUpdateSel, fullUpdateMVOffsets, err = buildFullUpdateLookupTemplateSelect(
			local.sctx,
			local.mvDBName,
			local.baseTable,
			local.MVSelect,
			local.mv.Columns,
			local.groupKeySet,
			local.groupKeyOffs,
			local.aggCols,
		)
		if err != nil {
			return nil, err
		}
		if fullUpdateSel.Fields == nil {
			return nil, errors.New("mvmerge: full-update lookup template has nil field list")
		}
		fullUpdateColumnCount = len(fullUpdateSel.Fields.Fields)
		if len(fullUpdateMVOffsets) != fullUpdateColumnCount {
			return nil, errors.Errorf(
				"mvmerge: full-update lookup template mv-offset mapping length mismatch: got %d, expected %d",
				len(fullUpdateMVOffsets),
				fullUpdateColumnCount,
			)
		}
	}

	mergeSel, deltaColumns, rowIDHandleOffset, err := buildMergeSourceSelect(
		local.mvDBName,
		local.mv,
		local.mv.Columns,
		local.groupKeySet,
		local.groupKeyOffs,
		deltaSel,
		local.aggCols,
	)
	if err != nil {
		return nil, err
	}

	expectedLen := len(local.mv.Columns) + len(deltaColumns)
	mvColumnOffsetBase := len(deltaColumns)
	if rowIDHandleOffset >= 0 {
		expectedLen++
	}

	// TODO: Revisit dependency-check ownership. We currently validate/derive aggregate dependencies
	// in planner mvmerge, but this may be moved to another stage (for example, MV creation-time checks)
	// after we evaluate end-to-end guarantees and maintenance cost.
	sumToCountExprIdx, err := mapSumToCountExprDependencies(local.aggCols, aggArgNotNullByOffset)
	if err != nil {
		return nil, err
	}
	minMaxToCountExprIdx, err := mapMinMaxToCountExprDependencies(local.aggCols, aggArgNotNullByOffset)
	if err != nil {
		return nil, err
	}

	// Patch delta offsets into AggInfos so executor can read delta payload by offset directly.
	deltaOffsetByName := make(map[string]int, len(deltaColumns))
	for _, dc := range deltaColumns {
		deltaOffsetByName[dc.Name] = dc.Offset
	}
	outAggInfos := make([]AggInfo, 0, len(local.aggCols))
	for i, ac := range local.aggCols {
		di := ac.info
		deps := make([]int, 0, 5)
		if ac.deltaName != "" {
			off, ok := deltaOffsetByName[ac.deltaName]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.deltaName)
			}
			deps = append(deps, off)
		}
		switch di.Kind {
		case AggSum:
			if !aggArgNotNullByOffset[di.MVOffset] {
				countIdx, ok := sumToCountExprIdx[i]
				if !ok {
					return nil, errors.Errorf("internal error: SUM at mv offset %d has no COUNT(expr) dependency", di.MVOffset)
				}
				countAgg := local.aggCols[countIdx]
				deps = append(deps, mvColumnOffsetBase+countAgg.info.MVOffset)
			}
		case AggMax, AggMin:
			addedCntOff, ok := deltaOffsetByName[ac.addedCountDeltaName]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.addedCountDeltaName)
			}
			removedValOff, ok := deltaOffsetByName[ac.removedValueDelta]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.removedValueDelta)
			}
			removedCntOff, ok := deltaOffsetByName[ac.removedCountDelta]
			if !ok {
				return nil, errors.Errorf("internal error: delta column %s not found in output", ac.removedCountDelta)
			}
			deps = append(deps, addedCntOff, removedValOff, removedCntOff)
			if !aggArgNotNullByOffset[di.MVOffset] {
				countIdx, ok := minMaxToCountExprIdx[i]
				if !ok {
					return nil, errors.Errorf("internal error: %v at mv offset %d has no COUNT(expr) dependency", di.Kind, di.MVOffset)
				}
				countAgg := local.aggCols[countIdx]
				deps = append(deps, mvColumnOffsetBase+countAgg.info.MVOffset)
			}
		}
		di.Dependencies = deps
		outAggInfos = append(outAggInfos, di)
	}
	// TODO: See the TODO above; dependency validation location may be adjusted in future.
	if err := validateAggDependencies(
		outAggInfos,
		mvColumnOffsetBase,
		len(local.mv.Columns),
		expectedLen,
	); err != nil {
		return nil, err
	}

	mvTablePKCols, err := buildMVTablePKHandleCols(local.mv, mvColumnOffsetBase, rowIDHandleOffset)
	if err != nil {
		return nil, err
	}

	res := &BuildResult{
		MergeSourceSelect:              mergeSel,
		SourceColumnCount:              expectedLen,
		FullUpdateLookupTemplateSelect: fullUpdateSel,
		FullUpdateLookupColumnCount:    fullUpdateColumnCount,
		FullUpdateLookupMVOffsets:      append([]int(nil), fullUpdateMVOffsets...),
		MVTableID:                      local.mv.ID,
		BaseTableID:                    local.baseTableID,
		MLogTableID:                    local.mlogTableID,
		MVColumnCount:                  len(local.mv.Columns),
		DeltaColumnCount:               len(deltaColumns),
		MVTablePKCols:                  mvTablePKCols,
		GroupKeyMVOffsets:              append([]int(nil), local.groupKeyOffs...),
		CountStarMVOffset:              local.countStarMVOffset,
		AggInfos:                       outAggInfos,
	}
	return res, nil
}

func buildMVTablePKHandleCols(
	mv *model.TableInfo,
	mvColumnOffsetBase, rowIDHandleOffset int,
) (plannerutil.HandleCols, error) {
	if mv == nil {
		return nil, errors.New("mv table info is nil")
	}

	mvOffsetByColID := make(map[int64]int, len(mv.Columns))
	for i, col := range mv.Columns {
		mvOffsetByColID[col.ID] = i
	}

	switch {
	case mv.PKIsHandle:
		pkCol := mv.GetPkColInfo()
		if pkCol == nil {
			return nil, errors.Errorf("mv table %s has PKIsHandle but no primary key column", mv.Name.O)
		}
		mvOffset, ok := mvOffsetByColID[pkCol.ID]
		if !ok {
			return nil, errors.Errorf("mv table %s primary key column id %d not found in mv columns", mv.Name.O, pkCol.ID)
		}
		return plannerutil.NewIntHandleCols(&expression.Column{
			ID:      pkCol.ID,
			RetType: &pkCol.FieldType,
			Index:   mvColumnOffsetBase + mvOffset,
		}), nil
	case mv.IsCommonHandle:
		pkIdx := tables.FindPrimaryIndex(mv)
		if pkIdx == nil {
			return nil, errors.Errorf("mv table %s has common handle but no primary index", mv.Name.O)
		}
		cols := make([]*expression.Column, 0, len(pkIdx.Columns))
		for _, idxCol := range pkIdx.Columns {
			if idxCol.Offset < 0 || idxCol.Offset >= len(mv.Columns) {
				return nil, errors.Errorf(
					"mv table %s primary index %s has invalid column offset %d",
					mv.Name.O,
					pkIdx.Name.O,
					idxCol.Offset,
				)
			}
			col := mv.Columns[idxCol.Offset]
			mvOffset, ok := mvOffsetByColID[col.ID]
			if !ok {
				return nil, errors.Errorf("mv table %s primary key column id %d not found in mv columns", mv.Name.O, col.ID)
			}
			cols = append(cols, &expression.Column{
				ID:      col.ID,
				RetType: &col.FieldType,
				Index:   mvColumnOffsetBase + mvOffset,
			})
		}
		return plannerutil.NewCommonHandlesColsWithoutColsAlign(mv, pkIdx, cols), nil
	default:
		if rowIDHandleOffset < 0 {
			return nil, errors.Errorf(
				"mv table %s uses extra row-id handle but merge-source output has no _tidb_rowid column",
				mv.Name.O,
			)
		}
		extraHandleCol := model.NewExtraHandleColInfo()
		return plannerutil.NewIntHandleCols(&expression.Column{
			ID:      extraHandleCol.ID,
			RetType: &extraHandleCol.FieldType,
			Index:   rowIDHandleOffset,
		}), nil
	}
}

// inferAggArgNotNullByOffset infers aggregate argument nullability from base-table column flags.
// If an aggregate argument is known NOT NULL, executor does not need COUNT(expr) dependencies.
func inferAggArgNotNullByOffset(local *buildLocalResult) map[int]bool {
	if local == nil || local.baseTable == nil {
		return nil
	}
	if len(local.baseTable.Columns) == 0 || len(local.aggCols) == 0 {
		return nil
	}

	baseColNotNull := make(map[string]bool, len(local.baseTable.Columns))
	for _, c := range local.baseTable.Columns {
		if c == nil {
			continue
		}
		baseColNotNull[c.Name.L] = mysql.HasNotNullFlag(c.GetFlag())
	}

	out := make(map[int]bool)
	for _, ac := range local.aggCols {
		if ac.info.ArgColName == "" {
			continue
		}
		if baseColNotNull[ac.info.ArgColName] {
			out[ac.info.MVOffset] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseSelectFromSQL(sctx planctx.PlanContext, sql string) (*ast.SelectStmt, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetParserConfig(sctx.GetSessionVars().BuildParserConfig())
	stmt, err := p.ParseOneStmt(sql, charset, collation)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, errors.Errorf("expected select statement, got %T", stmt)
	}
	return sel, nil
}

func parseExprFromSQL(sctx planctx.PlanContext, exprSQL string) (ast.ExprNode, error) {
	sel, err := parseSelectFromSQL(sctx, "select "+exprSQL)
	if err != nil {
		return nil, err
	}
	if sel.Fields == nil || len(sel.Fields.Fields) != 1 || sel.Fields.Fields[0] == nil {
		return nil, errors.New("failed to parse expression: expected one select field")
	}
	return sel.Fields.Fields[0].Expr, nil
}

func cloneExprByRestore(sctx planctx.PlanContext, expr ast.ExprNode) (ast.ExprNode, error) {
	if expr == nil {
		return nil, nil
	}
	exprSQL, err := restoreExprStrict(expr)
	if err != nil {
		return nil, err
	}
	return parseExprFromSQL(sctx, exprSQL)
}

func restoreExprStrict(expr ast.ExprNode) (string, error) {
	if expr == nil {
		return "", errors.New("expression is nil")
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(ctx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// extractGroupKeyOffsetsFromMVSelect maps GROUP BY columns back to their offsets in SELECT output.
// Those offsets define join keys between stage-1 deltas and MV snapshot rows.
func extractGroupKeyOffsetsFromMVSelect(sel *ast.SelectStmt) ([]int, error) {
	if sel.GroupBy == nil || len(sel.GroupBy.Items) == 0 {
		return nil, errors.New("materialized view definition must have GROUP BY")
	}
	offsetByColName := make(map[string]int, 8)
	for i, f := range sel.Fields.Fields {
		col, ok := f.Expr.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		offsetByColName[col.Name.Name.L] = i
	}
	groupOffsets := make([]int, 0, len(sel.GroupBy.Items))
	for _, item := range sel.GroupBy.Items {
		col, ok := item.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, errors.New("GROUP BY expression must be a column name for mvmerge")
		}
		off, ok := offsetByColName[col.Name.Name.L]
		if !ok {
			return nil, errors.Errorf("GROUP BY column %s must appear in SELECT list", col.Name.Name.O)
		}
		groupOffsets = append(groupOffsets, off)
	}
	return groupOffsets, nil
}

// extractAggInfosFromMVSelect parses supported aggregate expressions in MV SELECT list and
// prepares per-aggregate stage-1 delta column metadata.
func extractAggInfosFromMVSelect(sel *ast.SelectStmt) (aggCols []aggColInfo, hasMinMax bool, _ error) {
	for i, f := range sel.Fields.Fields {
		agg, ok := f.Expr.(*ast.AggregateFuncExpr)
		if !ok {
			continue
		}
		switch strings.ToLower(agg.F) {
		case ast.AggFuncCount:
			if len(agg.Args) != 1 {
				return nil, false, errors.New("COUNT must have exactly one argument for mvmerge stage-1")
			}
			if agg.Distinct {
				return nil, false, errors.New("COUNT(DISTINCT ...) is not supported in mvmerge stage-1")
			}
			if isCountStarOrOneArg(agg.Args[0]) {
				aggCols = append(aggCols, aggColInfo{
					info: AggInfo{
						Kind:     AggCountStar,
						MVOffset: i,
					},
					deltaName: deltaCntStarName,
				})
				continue
			}
			aggCols = append(aggCols, aggColInfo{
				info: func() AggInfo {
					ai := AggInfo{
						Kind:     AggCount,
						MVOffset: i,
					}
					if argCol, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
						ai.ArgColName = argCol.Name.Name.L
					}
					return ai
				}(),
				deltaName: fmt.Sprintf("__mvmerge_delta_cnt_%d", i),
				argExpr:   stripColumnQualifier(agg.Args[0]),
			})
		case ast.AggFuncSum:
			if len(agg.Args) != 1 {
				return nil, false, errors.New("SUM must have exactly one argument for mvmerge stage-1")
			}
			if agg.Distinct {
				return nil, false, errors.New("SUM(DISTINCT ...) is not supported in mvmerge stage-1")
			}
			ai := AggInfo{
				Kind:     AggSum,
				MVOffset: i,
			}
			if argCol, ok := agg.Args[0].(*ast.ColumnNameExpr); ok {
				ai.ArgColName = argCol.Name.Name.L
			}
			aggCols = append(aggCols, aggColInfo{
				info:      ai,
				deltaName: fmt.Sprintf("__mvmerge_delta_sum_%d", i),
				argExpr:   stripColumnQualifier(agg.Args[0]),
			})
		case ast.AggFuncMax:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MAX argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, aggColInfo{
				info: AggInfo{
					Kind:       AggMax,
					MVOffset:   i,
					ArgColName: argCol.Name.Name.L,
				},
				deltaName:           fmt.Sprintf("__mvmerge_max_in_added_%d", i),
				addedCountDeltaName: fmt.Sprintf("__mvmerge_max_cnt_in_added_%d", i),
				removedValueDelta:   fmt.Sprintf("__mvmerge_max_in_removed_%d", i),
				removedCountDelta:   fmt.Sprintf("__mvmerge_max_cnt_in_removed_%d", i),
				argExpr:             stripColumnQualifier(agg.Args[0]),
			})
		case ast.AggFuncMin:
			argCol, ok := agg.Args[0].(*ast.ColumnNameExpr)
			if !ok {
				return nil, false, errors.New("MIN argument must be a column name for mvmerge")
			}
			hasMinMax = true
			aggCols = append(aggCols, aggColInfo{
				info: AggInfo{
					Kind:       AggMin,
					MVOffset:   i,
					ArgColName: argCol.Name.Name.L,
				},
				deltaName:           fmt.Sprintf("__mvmerge_min_in_added_%d", i),
				addedCountDeltaName: fmt.Sprintf("__mvmerge_min_cnt_in_added_%d", i),
				removedValueDelta:   fmt.Sprintf("__mvmerge_min_in_removed_%d", i),
				removedCountDelta:   fmt.Sprintf("__mvmerge_min_cnt_in_removed_%d", i),
				argExpr:             stripColumnQualifier(agg.Args[0]),
			})
		default:
			return nil, false, errors.Errorf("unsupported aggregate function %s in mvmerge stage-1", agg.F)
		}
	}
	return aggCols, hasMinMax, nil
}

// mapSumToCountExprDependencies finds, for every nullable SUM(expr), the unique matching
// COUNT(expr) aggregate in the same MV SELECT list. The dependency is required to preserve
// SUM(NULL) semantics when applying incremental updates.
func mapSumToCountExprDependencies(aggCols []aggColInfo, sumArgNotNullByOffset map[int]bool) (map[int]int, error) {
	sumToCountIdx := make(map[int]int)
	for i, ac := range aggCols {
		if ac.info.Kind != AggSum {
			continue
		}
		if sumArgNotNullByOffset[ac.info.MVOffset] {
			continue
		}
		if ac.argExpr == nil {
			return nil, errors.Errorf("SUM aggregate argument is nil at mv offset %d", ac.info.MVOffset)
		}
		matchIdx := -1
		for j, cand := range aggCols {
			if cand.info.Kind != AggCount || cand.argExpr == nil {
				continue
			}
			if !exprStructuralEqual(ac.argExpr, cand.argExpr) {
				continue
			}
			if matchIdx >= 0 {
				return nil, errors.Errorf(
					"SUM expression %s at mv offset %d has multiple matching COUNT(expr) dependencies (offsets %d and %d)",
					restoreExpr(ac.argExpr), ac.info.MVOffset, aggCols[matchIdx].info.MVOffset, cand.info.MVOffset,
				)
			}
			matchIdx = j
		}
		if matchIdx < 0 {
			return nil, errors.Errorf(
				"SUM expression %s at mv offset %d requires matching COUNT(expr) in SELECT list",
				restoreExpr(ac.argExpr), ac.info.MVOffset,
			)
		}
		sumToCountIdx[i] = matchIdx
	}
	return sumToCountIdx, nil
}

func mapMinMaxToCountExprDependencies(aggCols []aggColInfo, aggArgNotNullByOffset map[int]bool) (map[int]int, error) {
	minMaxToCountIdx := make(map[int]int)
	for i, ac := range aggCols {
		if ac.info.Kind != AggMax && ac.info.Kind != AggMin {
			continue
		}
		if aggArgNotNullByOffset[ac.info.MVOffset] {
			continue
		}
		if ac.argExpr == nil {
			return nil, errors.Errorf("%v aggregate argument is nil at mv offset %d", ac.info.Kind, ac.info.MVOffset)
		}
		matchIdx := -1
		for j, cand := range aggCols {
			if cand.info.Kind != AggCount || cand.argExpr == nil {
				continue
			}
			if !exprStructuralEqual(ac.argExpr, cand.argExpr) {
				continue
			}
			if matchIdx >= 0 {
				return nil, errors.Errorf(
					"%v expression %s at mv offset %d has multiple matching COUNT(expr) dependencies (offsets %d and %d)",
					ac.info.Kind, restoreExpr(ac.argExpr), ac.info.MVOffset, aggCols[matchIdx].info.MVOffset, cand.info.MVOffset,
				)
			}
			matchIdx = j
		}
		if matchIdx < 0 {
			return nil, errors.Errorf(
				"%v expression %s at mv offset %d requires matching COUNT(expr) in SELECT list",
				ac.info.Kind, restoreExpr(ac.argExpr), ac.info.MVOffset,
			)
		}
		minMaxToCountIdx[i] = matchIdx
	}
	return minMaxToCountIdx, nil
}

// validateAggDependencies checks that each aggregate exposes the expected dependency layout and
// that every dependency offset points to a valid position in the final merge-source schema.
func validateAggDependencies(
	aggInfos []AggInfo,
	mvColumnOffsetBase int,
	mvColumnCount int,
	schemaLen int,
) error {
	mvColumnEnd := mvColumnOffsetBase + mvColumnCount
	for _, ai := range aggInfos {
		if ai.MVOffset < 0 || ai.MVOffset >= mvColumnCount {
			return errors.Errorf("invalid mv offset %d for %v", ai.MVOffset, ai.Kind)
		}
		for _, dep := range ai.Dependencies {
			if dep < 0 || dep >= schemaLen {
				return errors.Errorf("invalid dependency offset %d for %v at mv offset %d", dep, ai.Kind, ai.MVOffset)
			}
		}
		switch ai.Kind {
		case AggCountStar, AggCount:
			if len(ai.Dependencies) != 1 {
				return errors.Errorf(
					"%v at mv offset %d expects dependencies [self_delta], got %v",
					ai.Kind,
					ai.MVOffset,
					ai.Dependencies,
				)
			}
			if ai.Dependencies[0] >= mvColumnOffsetBase {
				return errors.Errorf(
					"%v at mv offset %d has invalid self_delta offset %d",
					ai.Kind,
					ai.MVOffset,
					ai.Dependencies[0],
				)
			}
		case AggSum:
			// [self_delta] when SUM argument is NOT NULL; otherwise [self_delta, matched_count_expr_mv].
			if len(ai.Dependencies) != 1 && len(ai.Dependencies) != 2 {
				return errors.Errorf(
					"SUM at mv offset %d expects dependencies [self_delta] or [self_delta, matched_count_expr_mv], got %v",
					ai.MVOffset,
					ai.Dependencies,
				)
			}
			if ai.Dependencies[0] >= mvColumnOffsetBase {
				return errors.Errorf("SUM at mv offset %d has invalid self_delta offset %d", ai.MVOffset, ai.Dependencies[0])
			}
			if len(ai.Dependencies) == 2 {
				if ai.Dependencies[1] < mvColumnOffsetBase || ai.Dependencies[1] >= mvColumnEnd {
					return errors.Errorf(
						"SUM at mv offset %d has invalid matched_count_expr_mv offset %d",
						ai.MVOffset,
						ai.Dependencies[1],
					)
				}
			}
		case AggMax, AggMin:
			// [added_val, added_cnt, removed_val, removed_cnt] + optional [matched_count_expr_mv] for nullable arg.
			if len(ai.Dependencies) != 4 && len(ai.Dependencies) != 5 {
				return errors.Errorf(
					"%v at mv offset %d expects dependencies "+
						"[added_val, added_cnt, removed_val, removed_cnt] or "+
						"[added_val, added_cnt, removed_val, removed_cnt, matched_count_expr_mv], got %v",
					ai.Kind,
					ai.MVOffset,
					ai.Dependencies,
				)
			}
			for depPos := 0; depPos < 4; depPos++ {
				if ai.Dependencies[depPos] >= mvColumnOffsetBase {
					return errors.Errorf(
						"%v at mv offset %d has invalid delta dependency[%d] offset %d",
						ai.Kind,
						ai.MVOffset,
						depPos,
						ai.Dependencies[depPos],
					)
				}
			}
			if len(ai.Dependencies) == 5 &&
				(ai.Dependencies[4] < mvColumnOffsetBase || ai.Dependencies[4] >= mvColumnEnd) {
				return errors.Errorf(
					"%v at mv offset %d has invalid matched_count_expr_mv offset %d",
					ai.Kind,
					ai.MVOffset,
					ai.Dependencies[4],
				)
			}
		default:
			return errors.Errorf("unsupported aggregate kind %v", ai.Kind)
		}
		if len(ai.Dependencies) == 0 {
			return errors.Errorf("%v at mv offset %d has empty Dependencies", ai.Kind, ai.MVOffset)
		}
	}
	return nil
}

func isCountStarOrOneArg(arg ast.ExprNode) bool {
	if arg == nil {
		return true
	}
	v, ok := arg.(ast.ValueExpr)
	if !ok {
		return false
	}
	switch x := v.GetValue().(type) {
	case int64:
		return x == 1
	case uint64:
		return x == 1
	case int:
		return x == 1
	default:
		return false
	}
}

func stripColumnQualifier(expr ast.ExprNode) ast.ExprNode {
	if expr == nil {
		return nil
	}
	expr.Accept(&columnQualifierStripper{})
	return stripAllParentheses(expr)
}

func exprStructuralEqual(lhs, rhs ast.ExprNode) bool {
	lhs = stripParentheses(lhs)
	rhs = stripParentheses(rhs)
	return reflectStructuralEqual(reflect.ValueOf(lhs), reflect.ValueOf(rhs))
}

func stripParentheses(expr ast.ExprNode) ast.ExprNode {
	for {
		p, ok := expr.(*ast.ParenthesesExpr)
		if !ok || p == nil || p.Expr == nil {
			return expr
		}
		expr = p.Expr
	}
}

func reflectStructuralEqual(lhs, rhs reflect.Value) bool {
	if !lhs.IsValid() || !rhs.IsValid() {
		return !lhs.IsValid() && !rhs.IsValid()
	}
	if lhs.Type() != rhs.Type() {
		return false
	}

	switch lhs.Kind() {
	case reflect.Interface, reflect.Pointer:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil()
		}
		return reflectStructuralEqual(lhs.Elem(), rhs.Elem())
	case reflect.Struct:
		typ := lhs.Type()
		for i := 0; i < lhs.NumField(); i++ {
			f := typ.Field(i)
			// Ignore unexported fields (e.g. parser location/cache fields).
			if f.PkgPath != "" {
				continue
			}
			if !reflectStructuralEqual(lhs.Field(i), rhs.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if lhs.Len() != rhs.Len() {
			return false
		}
		for i := 0; i < lhs.Len(); i++ {
			if !reflectStructuralEqual(lhs.Index(i), rhs.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if lhs.IsNil() || rhs.IsNil() {
			return lhs.IsNil() == rhs.IsNil()
		}
		return reflect.DeepEqual(lhs.Interface(), rhs.Interface())
	case reflect.Func:
		return lhs.IsNil() && rhs.IsNil()
	default:
		if lhs.CanInterface() && rhs.CanInterface() {
			return reflect.DeepEqual(lhs.Interface(), rhs.Interface())
		}
		return false
	}
}

func restoreExpr(expr ast.ExprNode) string {
	if expr == nil {
		return "<nil>"
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := expr.Restore(ctx); err != nil {
		return fmt.Sprintf("<restore error: %v>", err)
	}
	return sb.String()
}

func stripAllParentheses(expr ast.ExprNode) ast.ExprNode {
	if expr == nil {
		return nil
	}
	n, ok := expr.Accept(&parenthesesStripper{})
	if !ok {
		return expr
	}
	if e, ok := n.(ast.ExprNode); ok {
		return e
	}
	return expr
}

// buildMLogDeltaSelect builds the stage-1 delta aggregation SELECT on mlog rows.
//
// SQL shape (simplified):
//
//	SELECT
//	  <group keys from MV SELECT list>,
//	  SUM(old_new) AS __mvmerge_delta_cnt_star,
//	  SUM(IF(<count arg> IS NOT NULL, old_new, 0)) AS __mvmerge_delta_cnt_<i>,
//	  SUM(IF(old_new = 1, <sum arg>, -<sum arg>)) AS __mvmerge_delta_sum_<i>,
//	  MAX(IF(old_new = 1, <arg>, NULL)) AS __mvmerge_max_in_added_<i>,
//	  MIN(IF(old_new = 1, <arg>, NULL)) AS __mvmerge_min_in_added_<i>,
//	  SUM(IF(old_new = -1, 1, 0)) AS __mvmerge_removed_rows   -- only when MIN/MAX exists
//	FROM <db>.<mlog>
//	WHERE _tidb_commit_ts > FromTS
//	  AND _tidb_commit_ts <= ToTS
//	  AND <MV WHERE predicate>
//	GROUP BY <group keys from MV definition>
//
// old_new uses +1 for inserted/new rows and -1 for deleted/old rows.
// For COUNT/SUM, this sign directly encodes add/remove contribution.
func buildMLogDeltaSelect(
	sctx planctx.PlanContext,
	dbName pmodel.CIStr,
	mlogTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	mvCols []*model.ColumnInfo,
	groupKeyOffsets []int,
	aggCols []aggColInfo,
	opt BuildOptions,
) (*ast.SelectStmt, error) {
	buildMLogWhere := func() (ast.ExprNode, error) {
		tsCol := colExpr(model.ExtraCommitTSName.L)
		var where ast.ExprNode = binary(opcode.GT, tsCol, ast.NewValueExpr(opt.FromTS, "", ""))
		if mvSel.Where != nil {
			mvWhere, err := cloneExprByRestore(sctx, mvSel.Where)
			if err != nil {
				return nil, err
			}
			mvWhere.Accept(&columnQualifierStripper{})
			where = andExpr(where, stripAllParentheses(mvWhere))
		}
		return where, nil
	}

	groupKeyBaseColByMVOffset := make(map[int]string, len(groupKeyOffsets))
	groupBy := &ast.GroupByClause{Items: make([]*ast.ByItem, 0, len(groupKeyOffsets))}
	for _, mvOffset := range groupKeyOffsets {
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, err
		}
		groupKeyBaseColByMVOffset[mvOffset] = baseColExpr.Name.Name.O
		groupBy.Items = append(groupBy.Items, &ast.ByItem{Expr: baseColExpr, NullOrder: true})
	}

	phase1Fields := make([]*ast.SelectField, 0, len(groupKeyOffsets)+1+len(aggCols)+1)
	for _, mvOffset := range groupKeyOffsets {
		mvColName := mvCols[mvOffset].Name
		phase1Fields = append(phase1Fields, &ast.SelectField{
			Expr:   colExpr(groupKeyBaseColByMVOffset[mvOffset]),
			AsName: mvColName,
		})
	}
	oldNewCol := colExpr(model.MaterializedViewLogOldNewColumnName)
	phase1Fields = append(phase1Fields, &ast.SelectField{
		Expr:   aggSumInt(oldNewCol),
		AsName: pmodel.NewCIStr(deltaCntStarName),
	})

	for _, ac := range aggCols {
		switch ac.info.Kind {
		case AggCountStar:
			continue
		case AggCount:
			if ac.argExpr == nil {
				return nil, errors.New("COUNT aggregate argument is nil for mvmerge")
			}
			argExpr, err := cloneExprByRestore(sctx, ac.argExpr)
			if err != nil {
				return nil, err
			}
			cond := &ast.IsNullExpr{Expr: argExpr, Not: true}
			phase1Fields = append(phase1Fields, &ast.SelectField{
				Expr:   aggSumInt(ifExpr(cond, oldNewCol, ast.NewValueExpr(int64(0), "", ""))),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggSum:
			if ac.argExpr == nil {
				return nil, errors.New("SUM aggregate argument is nil for mvmerge")
			}
			argExpr, err := cloneExprByRestore(sctx, ac.argExpr)
			if err != nil {
				return nil, err
			}
			addedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", ""))
			phase1Fields = append(phase1Fields, &ast.SelectField{
				Expr:   aggSum(ifExpr(addedCond, argExpr, &ast.UnaryOperationExpr{Op: opcode.Minus, V: argExpr})),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		case AggMax:
			if ac.info.ArgColName == "" {
				return nil, errors.New("MAX aggregate argument column is empty for mvmerge")
			}
			argCol := colExpr(ac.info.ArgColName)
			addedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", ""))
			removedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(-1), "", ""))
			addedArg := ifExpr(addedCond, argCol, ast.NewValueExpr(nil, "", ""))
			removedArg := ifExpr(removedCond, argCol, ast.NewValueExpr(nil, "", ""))
			phase1Fields = append(phase1Fields,
				&ast.SelectField{
					Expr:   aggMax(addedArg),
					AsName: pmodel.NewCIStr(ac.deltaName),
				},
				&ast.SelectField{
					Expr:   aggMaxCount(addedArg),
					AsName: pmodel.NewCIStr(ac.addedCountDeltaName),
				},
				&ast.SelectField{
					Expr:   aggMax(removedArg),
					AsName: pmodel.NewCIStr(ac.removedValueDelta),
				},
				&ast.SelectField{
					Expr:   aggMaxCount(removedArg),
					AsName: pmodel.NewCIStr(ac.removedCountDelta),
				},
			)
		case AggMin:
			if ac.info.ArgColName == "" {
				return nil, errors.New("MIN aggregate argument column is empty for mvmerge")
			}
			argCol := colExpr(ac.info.ArgColName)
			addedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(1), "", ""))
			removedCond := binary(opcode.EQ, oldNewCol, ast.NewValueExpr(int64(-1), "", ""))
			addedArg := ifExpr(addedCond, argCol, ast.NewValueExpr(nil, "", ""))
			removedArg := ifExpr(removedCond, argCol, ast.NewValueExpr(nil, "", ""))
			phase1Fields = append(phase1Fields,
				&ast.SelectField{
					Expr:   aggMin(addedArg),
					AsName: pmodel.NewCIStr(ac.deltaName),
				},
				&ast.SelectField{
					Expr:   aggMinCount(addedArg),
					AsName: pmodel.NewCIStr(ac.addedCountDeltaName),
				},
				&ast.SelectField{
					Expr:   aggMin(removedArg),
					AsName: pmodel.NewCIStr(ac.removedValueDelta),
				},
				&ast.SelectField{
					Expr:   aggMinCount(removedArg),
					AsName: pmodel.NewCIStr(ac.removedCountDelta),
				},
			)
		default:
			return nil, errors.Errorf("unsupported agg kind %v", ac.info.Kind)
		}
	}

	mlogFrom := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{
		Source: &ast.TableName{Schema: dbName, Name: mlogTable.Name},
	}}}
	phase1Where, err := buildMLogWhere()
	if err != nil {
		return nil, err
	}
	return &ast.SelectStmt{
		Fields:  &ast.FieldList{Fields: phase1Fields},
		From:    mlogFrom,
		Where:   phase1Where,
		GroupBy: groupBy,
	}, nil
}

// buildMergeSourceSelect builds stage-2 SQL by joining stage-1 deltas with current MV rows.
//
// SQL shape (simplified):
//
//	SELECT
//	  delta.<all delta payload cols>,
//	  delta.<group-key mv columns>,   -- projected from delta side to keep changed groups
//	  mv.<non-group-key mv columns>,
//	  mv._tidb_rowid AS __mvmerge_mv_rowid  -- only for extra row-id handle tables
//	FROM (<stage-1 delta select>) AS delta
//	LEFT JOIN <db>.<mv> AS mv
//	  ON delta.<group_key_1> <=> mv.<group_key_1>
//	 AND ...
//
// Null-safe equality (<=>) keeps SQL NULL-group semantics consistent with GROUP BY keys.
// Delta is placed on the left side so every changed group survives even when MV row does not exist yet.
func buildMergeSourceSelect(
	dbName pmodel.CIStr,
	mv *model.TableInfo,
	mvCols []*model.ColumnInfo,
	groupKeySet map[int]struct{},
	groupKeyOffsets []int,
	deltaSel *ast.SelectStmt,
	aggCols []aggColInfo,
) (*ast.SelectStmt, []DeltaColumn, int, error) {
	deltaSrc := &ast.TableSource{Source: deltaSel, AsName: pmodel.NewCIStr(deltaTableAlias)}
	mvSrc := &ast.TableSource{
		Source: &ast.TableName{Schema: dbName, Name: mv.Name},
		AsName: pmodel.NewCIStr(mvTableAlias),
	}

	// Build null-safe join conditions on group keys.
	var onExpr ast.ExprNode
	for _, mvOffset := range groupKeyOffsets {
		colName := mvCols[mvOffset].Name
		deltaCol := qualColExpr(deltaTableAlias, colName.O)
		mvCol := qualColExpr(mvTableAlias, colName.O)
		cmp := binary(opcode.NullEQ, deltaCol, mvCol)
		if onExpr == nil {
			onExpr = cmp
		} else {
			onExpr = andExpr(onExpr, cmp)
		}
	}
	if onExpr == nil {
		return nil, nil, -1, errors.New("empty join key for mvmerge")
	}

	// Use delta as left side so every changed group survives even when MV row doesn't exist yet.
	join := &ast.Join{
		Left:  deltaSrc,
		Right: mvSrc,
		Tp:    ast.LeftJoin,
		On:    &ast.OnCondition{Expr: onExpr},
	}

	// SELECT: delta columns first, then MV columns.
	// Group keys in MV layout are still MV columns conceptually, but projected from delta side directly.
	// delta is the LEFT side and always exists for changed groups.
	fields := make([]*ast.SelectField, 0, len(mvCols)+1+len(aggCols)+1)

	deltaColumns := make([]DeltaColumn, 0, 1+len(aggCols))
	nextOffset := 0
	// Keep all delta columns contiguous before MV columns, and track their absolute output offsets.
	rowIDHandleOffset := -1
	addDeltaCol := func(name string) int {
		off := nextOffset
		nextOffset++
		deltaColumns = append(deltaColumns, DeltaColumn{Name: name, Offset: off})
		return off
	}

	// delta_cnt_star is always included.
	addDeltaCol(deltaCntStarName)
	fields = append(fields, &ast.SelectField{
		Expr:   qualColExpr(deltaTableAlias, deltaCntStarName),
		AsName: pmodel.NewCIStr(deltaCntStarName),
	})
	for _, ac := range aggCols {
		switch ac.info.Kind {
		case AggCountStar:
			continue
		case AggMax, AggMin:
			names := []string{ac.deltaName, ac.addedCountDeltaName, ac.removedValueDelta, ac.removedCountDelta}
			for _, name := range names {
				addDeltaCol(name)
				fields = append(fields, &ast.SelectField{
					Expr:   qualColExpr(deltaTableAlias, name),
					AsName: pmodel.NewCIStr(name),
				})
			}
		default:
			addDeltaCol(ac.deltaName)
			fields = append(fields, &ast.SelectField{
				Expr:   qualColExpr(deltaTableAlias, ac.deltaName),
				AsName: pmodel.NewCIStr(ac.deltaName),
			})
		}
	}

	for i, col := range mvCols {
		if _, ok := groupKeySet[i]; ok {
			fields = append(fields, &ast.SelectField{
				Expr:   qualColExpr(deltaTableAlias, col.Name.O),
				AsName: col.Name,
			})
			continue
		}
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(mvTableAlias, col.Name.O),
			AsName: col.Name,
		})
	}
	// Keep extra row-id handle outside delta payload so delta offsets and MV offsets stay stable.
	if !mv.PKIsHandle && !mv.IsCommonHandle {
		rowIDHandleOffset = len(fields)
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(mvTableAlias, model.ExtraHandleName.O),
			AsName: pmodel.NewCIStr(mvRowIDName),
		})
	}

	return &ast.SelectStmt{
		Fields: &ast.FieldList{Fields: fields},
		From:   &ast.TableRefsClause{TableRefs: join},
	}, deltaColumns, rowIDHandleOffset, nil
}

func buildFullUpdateLookupTemplateSelect(
	sctx planctx.PlanContext,
	dbName pmodel.CIStr,
	baseTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	mvCols []*model.ColumnInfo,
	groupKeySet map[int]struct{},
	groupKeyOffsets []int,
	aggCols []aggColInfo,
) (*ast.SelectStmt, []int, error) {
	outerSel, outerGKAliasByMVOffset, err := buildFullUpdateLookupOuterSelect(
		sctx,
		dbName,
		baseTable,
		mvSel,
		groupKeyOffsets,
	)
	if err != nil {
		return nil, nil, err
	}
	groupKeyBaseColByMVOffset := make(map[int]string, len(groupKeyOffsets))
	for _, mvOffset := range groupKeyOffsets {
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, nil, err
		}
		groupKeyBaseColByMVOffset[mvOffset] = baseColExpr.Name.Name.O
	}
	aggKindByMVOffset := make(map[int]AggKind, len(aggCols))
	for _, ac := range aggCols {
		aggKindByMVOffset[ac.info.MVOffset] = ac.info.Kind
	}
	innerSel, err := buildFullUpdateLookupInnerSelect(
		sctx,
		dbName,
		baseTable,
		mvSel,
		mvCols,
		groupKeySet,
		groupKeyOffsets,
		groupKeyBaseColByMVOffset,
		aggCols,
	)
	if err != nil {
		return nil, nil, err
	}

	outerSrc := &ast.TableSource{Source: outerSel, AsName: pmodel.NewCIStr(fullUpdateOuterAlias)}
	innerSrc := &ast.TableSource{Source: innerSel, AsName: pmodel.NewCIStr(fullUpdateInnerAlias)}

	var onExpr ast.ExprNode
	for _, mvOffset := range groupKeyOffsets {
		outerGK := qualColExpr(fullUpdateOuterAlias, outerGKAliasByMVOffset[mvOffset])
		innerGK := qualColExpr(fullUpdateInnerAlias, groupKeyBaseColByMVOffset[mvOffset])
		// Group keys can be NULL, so full-update lookup must use null-safe equality.
		onExpr = andExpr(onExpr, binary(opcode.NullEQ, outerGK, innerGK))
	}
	if onExpr == nil {
		return nil, nil, errors.New("mvmerge: empty group key offsets for full-update lookup template")
	}

	fields := make([]*ast.SelectField, 0, len(mvCols))
	mvOffsets := make([]int, 0, len(mvCols))
	for mvOffset, mvCol := range mvCols {
		// Full-update fallback only returns group keys and MIN/MAX aggregate columns.
		if kind, ok := aggKindByMVOffset[mvOffset]; ok && kind != AggMin && kind != AggMax {
			continue
		}
		outColName := mvCol.Name.O
		if baseColName, ok := groupKeyBaseColByMVOffset[mvOffset]; ok {
			outColName = baseColName
		}
		fields = append(fields, &ast.SelectField{
			Expr:   qualColExpr(fullUpdateInnerAlias, outColName),
			AsName: mvCol.Name,
		})
		mvOffsets = append(mvOffsets, mvOffset)
	}

	return &ast.SelectStmt{
		Fields: &ast.FieldList{Fields: fields},
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left:  outerSrc,
			Right: innerSrc,
			Tp:    ast.CrossJoin,
			On:    &ast.OnCondition{Expr: onExpr},
		}},
		TableHints: []*ast.TableOptimizerHint{
			{
				// Keep the template stable on index-join path so planbuilder can extract
				// inner child + range/key mapping as executor rebuild metadata.
				HintName: pmodel.NewCIStr("inl_join"),
				Tables: []ast.HintTable{
					{TableName: pmodel.NewCIStr(fullUpdateOuterAlias)},
					{TableName: pmodel.NewCIStr(fullUpdateInnerAlias)},
				},
			},
		},
	}, mvOffsets, nil
}

func buildFullUpdateLookupOuterSelect(
	sctx planctx.PlanContext,
	dbName pmodel.CIStr,
	baseTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	groupKeyOffsets []int,
) (*ast.SelectStmt, map[int]string, error) {
	if baseTable == nil {
		return nil, nil, errors.New("mvmerge: base table is nil")
	}
	if len(groupKeyOffsets) == 0 {
		return nil, nil, errors.New("mvmerge: empty group key offsets for full-update lookup template")
	}

	fields := make([]*ast.SelectField, 0, len(groupKeyOffsets))
	groupKeyAliasByMVOffset := make(map[int]string, len(groupKeyOffsets))
	for _, mvOffset := range groupKeyOffsets {
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, nil, err
		}
		alias := fmt.Sprintf("__mvmerge_full_outer_gk_%d", mvOffset)
		groupKeyAliasByMVOffset[mvOffset] = alias
		fields = append(fields, &ast.SelectField{
			Expr:   baseColExpr,
			AsName: pmodel.NewCIStr(alias),
		})
	}

	var where ast.ExprNode
	if mvSel.Where != nil {
		mvWhere, err := cloneExprByRestore(sctx, mvSel.Where)
		if err != nil {
			return nil, nil, err
		}
		mvWhere.Accept(&columnQualifierStripper{})
		where = stripAllParentheses(mvWhere)
	}

	return &ast.SelectStmt{
		Fields: &ast.FieldList{Fields: fields},
		From: &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{
			Source: &ast.TableName{Schema: dbName, Name: baseTable.Name},
		}}},
		Where: where,
		// Outer side only provides one probe key tuple; runtime will refill real keys per changed group.
		Limit: &ast.Limit{Count: ast.NewValueExpr(int64(1), "", "")},
	}, groupKeyAliasByMVOffset, nil
}

func buildFullUpdateLookupInnerSelect(
	sctx planctx.PlanContext,
	dbName pmodel.CIStr,
	baseTable *model.TableInfo,
	mvSel *ast.SelectStmt,
	mvCols []*model.ColumnInfo,
	groupKeySet map[int]struct{},
	groupKeyOffsets []int,
	groupKeyBaseColByMVOffset map[int]string,
	aggCols []aggColInfo,
) (*ast.SelectStmt, error) {
	if baseTable == nil {
		return nil, errors.New("mvmerge: base table is nil")
	}
	if len(groupKeyOffsets) == 0 {
		return nil, errors.New("mvmerge: empty group key offsets for full-update lookup template")
	}

	aggByMVOffset := make(map[int]aggColInfo, len(aggCols))
	for _, ac := range aggCols {
		aggByMVOffset[ac.info.MVOffset] = ac
	}

	fields := make([]*ast.SelectField, 0, len(mvCols))
	for i, mvCol := range mvCols {
		if _, ok := groupKeySet[i]; ok {
			baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, i)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &ast.SelectField{
				Expr:   baseColExpr,
				AsName: pmodel.NewCIStr(groupKeyBaseColByMVOffset[i]),
			})
			continue
		}
		ac, ok := aggByMVOffset[i]
		if !ok {
			return nil, errors.Errorf("mv offset %d is neither group key nor aggregate", i)
		}
		// Full-update fallback only recomputes MIN/MAX aggregates.
		if ac.info.Kind != AggMin && ac.info.Kind != AggMax {
			continue
		}
		fullAggExpr, err := buildFullUpdateAggExpr(sctx, ac)
		if err != nil {
			return nil, err
		}
		fields = append(fields, &ast.SelectField{Expr: fullAggExpr, AsName: mvCol.Name})
	}

	var where ast.ExprNode
	if mvSel.Where != nil {
		mvWhere, err := cloneExprByRestore(sctx, mvSel.Where)
		if err != nil {
			return nil, err
		}
		mvWhere.Accept(&columnQualifierStripper{})
		where = stripAllParentheses(mvWhere)
	}

	groupBy := &ast.GroupByClause{Items: make([]*ast.ByItem, 0, len(groupKeyOffsets))}
	for _, mvOffset := range groupKeyOffsets {
		baseColExpr, err := groupKeyBaseColExprAtOffset(mvSel, mvOffset)
		if err != nil {
			return nil, err
		}
		groupBy.Items = append(groupBy.Items, &ast.ByItem{
			Expr:      baseColExpr,
			NullOrder: true,
		})
	}

	return &ast.SelectStmt{
		Fields: &ast.FieldList{Fields: fields},
		From: &ast.TableRefsClause{TableRefs: &ast.Join{
			Left: &ast.TableSource{
				Source: &ast.TableName{Schema: dbName, Name: baseTable.Name},
			},
		}},
		Where:   where,
		GroupBy: groupBy,
	}, nil
}

func buildFullUpdateAggExpr(sctx planctx.PlanContext, ac aggColInfo) (ast.ExprNode, error) {
	switch ac.info.Kind {
	case AggCountStar:
		return aggCount(ast.NewValueExpr(int64(1), "", "")), nil
	case AggCount:
		if ac.argExpr == nil {
			return nil, errors.New("COUNT aggregate argument is nil for full-update")
		}
		argExpr, err := cloneExprByRestore(sctx, ac.argExpr)
		if err != nil {
			return nil, err
		}
		return aggCount(argExpr), nil
	case AggSum:
		if ac.argExpr == nil {
			return nil, errors.New("SUM aggregate argument is nil for full-update")
		}
		argExpr, err := cloneExprByRestore(sctx, ac.argExpr)
		if err != nil {
			return nil, err
		}
		return aggSum(argExpr), nil
	case AggMax:
		if ac.info.ArgColName == "" {
			return nil, errors.New("MAX aggregate argument column is empty for full-update")
		}
		return aggMax(colExpr(ac.info.ArgColName)), nil
	case AggMin:
		if ac.info.ArgColName == "" {
			return nil, errors.New("MIN aggregate argument column is empty for full-update")
		}
		return aggMin(colExpr(ac.info.ArgColName)), nil
	default:
		return nil, errors.Errorf("unsupported agg kind %v for full-update", ac.info.Kind)
	}
}

// groupKeyBaseColExprAtOffset returns the underlying base-table column expression for one MV
// output offset. buildMLogDeltaSelect uses it to keep stage-1 GROUP BY aligned with MV layout.
func groupKeyBaseColExprAtOffset(mvSel *ast.SelectStmt, mvOffset int) (*ast.ColumnNameExpr, error) {
	if mvOffset < 0 || mvOffset >= len(mvSel.Fields.Fields) {
		return nil, errors.Errorf("invalid mv offset %d", mvOffset)
	}
	f := mvSel.Fields.Fields[mvOffset]
	col, ok := f.Expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil, errors.Errorf("mv field at offset %d is not a group key column", mvOffset)
	}
	// Return a stripped (unqualified) column name expression.
	return colExpr(col.Name.Name.O), nil
}

func dbNameByTableID(is infoschema.InfoSchema, tableID int64) (pmodel.CIStr, error) {
	item, ok := is.TableItemByID(tableID)
	if !ok {
		return pmodel.CIStr{}, errors.Errorf("table id %d not found in infoschema", tableID)
	}
	if item.DBName.L == "" {
		return pmodel.CIStr{}, errors.Errorf("table id %d has empty schema name in infoschema", tableID)
	}
	return item.DBName, nil
}

func hasColumn(t *model.TableInfo, colName string) bool {
	colName = strings.ToLower(colName)
	for _, c := range t.Columns {
		if c.Name.L == colName {
			return true
		}
	}
	return false
}

type columnQualifierStripper struct{}

func (*columnQualifierStripper) Enter(n ast.Node) (ast.Node, bool) {
	cn, ok := n.(*ast.ColumnNameExpr)
	if !ok || cn.Name == nil {
		return n, false
	}
	cn.Name.Schema = pmodel.CIStr{}
	cn.Name.Table = pmodel.CIStr{}
	return n, false
}

func (*columnQualifierStripper) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

type parenthesesStripper struct{}

func (*parenthesesStripper) Enter(n ast.Node) (ast.Node, bool) {
	return n, false
}

func (*parenthesesStripper) Leave(n ast.Node) (ast.Node, bool) {
	if p, ok := n.(*ast.ParenthesesExpr); ok && p.Expr != nil {
		return p.Expr, true
	}
	return n, true
}

func qualColExpr(tableAlias string, colName string) *ast.ColumnNameExpr {
	return &ast.ColumnNameExpr{Name: &ast.ColumnName{
		Table: pmodel.NewCIStr(tableAlias),
		Name:  pmodel.NewCIStr(colName),
	}}
}

func colExpr(colName string) *ast.ColumnNameExpr {
	return &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: pmodel.NewCIStr(colName)}}
}

func binary(op opcode.Op, l, r ast.ExprNode) *ast.BinaryOperationExpr {
	return &ast.BinaryOperationExpr{Op: op, L: l, R: r}
}

func andExpr(l, r ast.ExprNode) ast.ExprNode {
	if l == nil {
		return r
	}
	if r == nil {
		return l
	}
	return binary(opcode.LogicAnd, l, r)
}

func aggSum(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncSum, Args: []ast.ExprNode{arg}}
}

func aggCount(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncCount, Args: []ast.ExprNode{arg}}
}

func aggSumInt(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncSumInt, Args: []ast.ExprNode{arg}}
}

func aggMax(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMax, Args: []ast.ExprNode{arg}}
}

func aggMin(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMin, Args: []ast.ExprNode{arg}}
}

func aggMaxCount(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMaxCount, Args: []ast.ExprNode{arg}}
}

func aggMinCount(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncMinCount, Args: []ast.ExprNode{arg}}
}

func aggFirstRow(arg ast.ExprNode) *ast.AggregateFuncExpr {
	return &ast.AggregateFuncExpr{F: ast.AggFuncFirstRow, Args: []ast.ExprNode{arg}}
}

func ifExpr(cond, trueExpr, falseExpr ast.ExprNode) *ast.FuncCallExpr {
	return &ast.FuncCallExpr{
		Tp:     ast.FuncCallExprTypeGeneric,
		FnName: pmodel.NewCIStr("IF"),
		Args:   []ast.ExprNode{cond, trueExpr, falseExpr},
	}
}
