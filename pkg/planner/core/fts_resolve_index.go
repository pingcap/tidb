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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tipb/go-tipb"
	"k8s.io/utils/pointer"
)

var (
	ftsDistanceType types.FieldType
)

func init() {
	ftsDistanceType = *types.NewFieldType(mysql.TypeFloat)
	ftsDistanceType.SetFlag(mysql.NotNullFlag)
}

// FullTextIndexPlanVisitor is a visitor for traversing logical plans to handle full-text search index optimization.
type FullTextIndexPlanVisitor struct {
	parents           []base.LogicalPlan
	onEnterDataSource func(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error)
}

func (v *FullTextIndexPlanVisitor) getParent(n int) base.LogicalPlan {
	idx := len(v.parents) - 1 - n
	if idx >= 0 {
		return v.parents[idx]
	}
	return nil
}

func (v *FullTextIndexPlanVisitor) visit(plan base.LogicalPlan) (bool, error) {
	switch x := plan.(type) {
	case *logicalop.DataSource:
		return v.onEnterDataSource(v, x)
	}

	v.parents = append(v.parents, plan)
	children := plan.Children() // plan.Children() may be re-assigned, so we copy it out before visiting.
	for _, child := range children {
		if _, err := v.visit(child); err != nil {
			return false, err
		}
	}
	v.parents = v.parents[:len(v.parents)-1]

	return false, nil
}

// FullTextIndexResolverWhere resolves full-text search expressions in WHERE clauses and pushes them down to table scans.
// Must run before predicate pushdown.
type FullTextIndexResolverWhere struct{}

// Name returns the name of this optimization rule.
func (o *FullTextIndexResolverWhere) Name() string {
	return "fts_resolve_index_where"
}

// Optimize applies full-text search index resolution optimization to the logical plan.
func (o *FullTextIndexResolverWhere) Optimize(ctx context.Context, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{
		onEnterDataSource: o.onEnterDataSource,
	}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (o *FullTextIndexResolverWhere) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	// Example pattern:
	//
	//	SELECT ... FROM <TABLE>
	//	WHERE FTS_MATCH_WORD(...)
	//
	//	Input Plan:   DataSource          -> Selection
	//	Target Plan:  DataSource(→#Score) -> Selection(rest)

	// Extract FTS expr from the selection
	parent0 := v.getParent(0)
	if parent0 == nil {
		return false, nil
	}
	planSelection, ok := parent0.(*logicalop.LogicalSelection)
	if !ok {
		return false, nil
	}
	conds := planSelection.Conditions
	if len(conds) == 0 {
		return false, nil
	}
	var ftsInfo *expression.FTSInfo = nil
	newCond := make([]expression.Expression, 0, len(conds)) // The cond without FTS expr
	for idx, expr := range conds {
		ftsInfo = expression.InterpretFullTextSearchExpr(expr)
		if ftsInfo != nil {
			newCond = append(newCond, conds[:idx]...)
			newCond = append(newCond, conds[idx+1:]...)
			break
		}
	}
	if ftsInfo == nil {
		return false, nil
	}

	// Check FTS index with FTS expr
	var matchingIndex *model.IndexInfo = nil
	for _, idx := range ds.TableInfo.Indices {
		if idx.FullTextInfo == nil {
			continue
		}
		if len(idx.Columns) != 1 {
			continue
		}
		if ds.TableInfo.Columns[idx.Columns[0].Offset].ID != ftsInfo.Column.ID {
			continue
		}
		matchingIndex = idx
	}
	if matchingIndex == nil {
		// Fail fast: We recognized a valid FTS expr, but there is no matching index.
		return false, plannererrors.ErrWrongUsage.FastGen("Full text search can only be used with a matching fulltext index")
	}

	// Match succeeded, start push down FTS index.

	ds.FtsPushDown = &tipb.FTSQueryInfo{
		QueryType:      tipb.FTSQueryType_FTSQueryTypeWithScore,
		IndexId:        matchingIndex.ID,
		Columns:        []*tipb.ColumnInfo{util.ColumnToProto(ftsInfo.Column.ToInfo(), false, false)},
		ColumnNames:    []string{ftsInfo.Column.OrigName},
		QueryText:      ftsInfo.Query,
		QueryTokenizer: string(matchingIndex.FullTextInfo.ParserType),
		TopK:           pointer.Uint32(4294967295),
	}
	ds.Columns = append(ds.Columns, &model.ColumnInfo{
		Name:      pmodel.NewCIStr("_FTS_SCORE"), // Only for explain output
		ID:        model.VirtualColFTSScoreID,
		FieldType: ftsDistanceType,
	})
	// Append a score column to the schema.
	virtualScoreColumn := &expression.Column{
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  ftsDistanceType.Clone(),
	}
	ds.Schema().Append(virtualScoreColumn)
	// Eliminate the FTS expr from the selection condition.
	planSelection.Conditions = newCond
	// Remove planSelection itself if there is no remaining condition after push down the FTS index.
	if len(planSelection.Conditions) == 0 {
		parent1 := v.getParent(1) // The parent of the selection
		if parent1 != nil {
			idxToRemove := -1
			for idx, child := range parent1.Children() {
				if child == planSelection {
					idxToRemove = idx
					break
				}
			}
			if idxToRemove >= 0 {
				// We should not mutate the parent1.Children slice directly,
				// because it is still in the visiting process.
				// Here we reassign children to something new, so that the visitor
				// will still continue visiting the old slice without being affected.
				newChildren := make([]base.LogicalPlan, 0, len(parent1.Children()))
				newChildren = append(newChildren, parent1.Children()[:idxToRemove]...)
				newChildren = append(newChildren, planSelection.Children()...) // Substitute planSelection with its children
				newChildren = append(newChildren, parent1.Children()[idxToRemove+1:]...)
				parent1.SetChildren(newChildren...)
			}
		}
	}

	// Note: We intentionally leave the FTS column available for pruning, by removing the FTS expr from Selection.
	// This is exactly what we want.

	return true, nil
}

// FullTextIndexResolverTopN resolves full-text search expressions in ORDER BY clauses with LIMIT.
// Must run after TopN rewrite.
type FullTextIndexResolverTopN struct{}

// Name returns the name of this optimization rule.
func (o *FullTextIndexResolverTopN) Name() string {
	return "fts_resolve_index_topn"
}

// Optimize applies TopN optimization for full-text search queries.
func (o *FullTextIndexResolverTopN) Optimize(ctx context.Context, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{
		onEnterDataSource: o.onEnterDataSource,
	}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (o *FullTextIndexResolverTopN) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	// WHERE rewrite is not succeeded, just skip.
	if ds.FtsPushDown == nil {
		return false, nil
	}

	// Example pattern (common):
	//
	//	SELECT ... FROM <TABLE>
	//	WHERE FTS_MATCH_WORD(...)
	//	ORDER BY FTS_MATCH_WORD(...) LIMIT 10
	//
	//	Input Plan (After ResolverWhere):
	// 		DataSource(→#Score) -> Selection(rest)      -> TopN(by→#Score2)
	// 		DataSource(→#Score)                         -> TopN(by→#Score2)

	// It is possible that there is a Selection above logicalop.DataSource, or there may be no.
	parent0 := v.getParent(0)
	if parent0 == nil {
		return false, nil
	}
	var planSelection *logicalop.LogicalSelection = nil
	var planTopN *logicalop.LogicalTopN = nil
	var ok bool
	planSelection, ok = parent0.(*logicalop.LogicalSelection)
	if ok {
		parent1 := v.getParent(1)
		if parent1 == nil {
			return false, nil
		}
		planTopN, ok = parent1.(*logicalop.LogicalTopN)
	} else {
		planTopN, ok = parent0.(*logicalop.LogicalTopN)
	}
	if !ok {
		return false, nil
	}

	// The FTS() in ORDER BY must match the FTS() in WHERE.
	orderByExpr := planTopN.ByItems[0].Expr
	ftsInfoOrderBy := expression.InterpretFullTextSearchExpr(orderByExpr)
	if ftsInfoOrderBy == nil {
		return false, nil
	}
	if ftsInfoOrderBy.Column.ID != ds.FtsPushDown.Columns[0].ColumnId {
		return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in ORDER BY must match the one in WHERE")
	}
	if ftsInfoOrderBy.Query != ds.FtsPushDown.QueryText {
		return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in ORDER BY must match the one in WHERE")
	}

	// All check passed, rewrite the TopN to utilize the Score column (which is generated by Optimization1).

	planTopN.ByItems[0].Expr = ds.Schema().Columns[len(ds.Schema().Columns)-1]

	// A smaller TopK can be pushed down to TiFlash table scan when:
	// 1. there is no selection after table scan.
	// 2. order by the score in descending order.
	if planSelection == nil && len(ds.PushedDownConds) == 0 && planTopN.ByItems[0].Desc {
		ds.FtsPushDown.TopK = pointer.Uint32(uint32(planTopN.Offset + planTopN.Count))
	}

	return false, nil
}

// FullTextIndexResolverProjection must run after TopN rewrite
type FullTextIndexResolverProjection struct{}

// Name returns the name of this optimization rule.
func (o *FullTextIndexResolverProjection) Name() string {
	return "fts_resolve_index_projection"
}

// Optimize applies Projection optimization for full-text search queries.
func (o *FullTextIndexResolverProjection) Optimize(ctx context.Context, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{
		onEnterDataSource: o.onEnterDataSource,
	}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (o *FullTextIndexResolverProjection) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	// WHERE rewrite is not succeeded, just skip.
	if ds.FtsPushDown == nil {
		return false, nil
	}

	//	Input Plan (After ResolverTopN):
	// 		DataSource              -> Projection  	             |  SELECT FTS_MATCH_WORD(..) FROM t WHERE FTS_MATCH_WORD(..)
	// 		DataSource -> Selection -> Projection
	// 		DataSource              -> TopN       -> Projection  |  SELECT FTS_MATCH_WORD(..) FROM t WHERE FTS_MATCH_WORD(..) ORDER BY FTS_MATCH_WORD(..) DESC LIMIT 10
	// 		DataSource -> Selection -> TopN       -> Projection

	// We only accept 4 patterns as above.

	var planSelection *logicalop.LogicalSelection = nil
	var planTopN *logicalop.LogicalTopN = nil
	var planProjection *logicalop.LogicalProjection = nil

	for i := len(v.parents) - 1; i >= 0; i-- {
		stopHere := false
		switch p := v.parents[i].(type) {
		case *logicalop.LogicalSelection:
			if planTopN != nil || planProjection != nil || planSelection != nil {
				return false, nil
			}
			planSelection = p
		case *logicalop.LogicalTopN:
			if planTopN != nil || planProjection != nil {
				return false, nil
			}
			planTopN = p
		case *logicalop.LogicalProjection:
			planProjection = p
			stopHere = true
		default:
			return false, nil // Unsupported executor
		}
		if stopHere {
			break
		}
	}
	if planProjection == nil {
		// No projection, just skip.
		return false, nil
	}

	// The FTS() in SELECT must match the FTS() in WHERE. However we accept multiple FTS() in SELECT.
	matchedProjections := make([]int, 0, len(planProjection.Exprs))
	for i, expr := range planProjection.Exprs {
		ftsInfo := expression.InterpretFullTextSearchExpr(expr)
		if ftsInfo == nil {
			continue
		}
		if ftsInfo.Column.ID != ds.FtsPushDown.Columns[0].ColumnId {
			return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		}
		if ftsInfo.Query != ds.FtsPushDown.QueryText {
			return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		}
		matchedProjections = append(matchedProjections, i)
	}
	if len(matchedProjections) == 0 {
		return false, nil
	}

	// Replace the FTS expr with the score column.
	for _, idx := range matchedProjections {
		planProjection.Exprs[idx] = ds.Schema().Columns[len(ds.Schema().Columns)-1]
	}
	return true, nil
}

// FullTextIndexResolverRejectRemaining validates and rejects unsupported full-text search usage patterns.
// Must run after TopN rewrite.
type FullTextIndexResolverRejectRemaining struct{}

// Name returns the name of this optimization rule.
func (o *FullTextIndexResolverRejectRemaining) Name() string {
	return "fts_resolve_reject_remaining"
}

// Optimize validates full-text search usage and rejects unsupported patterns.
func (o *FullTextIndexResolverRejectRemaining) Optimize(ctx context.Context, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	if err := o.visit(plan); err != nil {
		return nil, false, err
	}
	return plan, false, nil
}

func (o *FullTextIndexResolverRejectRemaining) visit(plan base.LogicalPlan) error {
	switch p := plan.(type) {
	case *logicalop.LogicalProjection:
		for _, expr := range p.Exprs {
			if expression.ContainsFullTextSearchFn(expr) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in SELECT must not be placed inside any other function or expression, and must be used with a corresponding 'FTS_MATCH_WORD()' in WHERE. A valid example: SELECT FTS_MATCH_WORD(...) FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.DataSource:
		for _, item := range p.PushedDownConds {
			if expression.ContainsFullTextSearchFn(item) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in WHERE must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT ... FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.LogicalSelection:
		for _, cond := range p.Conditions {
			if expression.ContainsFullTextSearchFn(cond) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in WHERE must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT ... FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.LogicalTopN:
		for i, item := range p.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
				if i > 0 {
					return plannererrors.ErrWrongUsage.FastGen("FTS_MATCH_WORD() must be used as the first item in ORDER BY. A valid example: SELECT ... FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT ..")
				}
				// Two possibilities that we still meet a FTS expr here:
				// 1. There is no WHERE at all
				// 2. There is a WHERE, but FTS in TopN cannot be pushed down
				return plannererrors.ErrWrongUsage.FastGen("Unsupported 'FTS_MATCH_WORD()' usage. It must be used with a WHERE clause and must be used alone. A valid example: SELECT ... FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT ..")
			}
		}
	case *logicalop.LogicalSort:
		for _, item := range p.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' in ORDER BY without a LIMIT clause is not supported, try specify a very large LIMIT as a workaround")
			}
		}
	}
	for _, child := range plan.Children() {
		if err := o.visit(child); err != nil {
			return err
		}
	}
	return nil
}
