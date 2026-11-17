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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tipb/go-tipb"
)

const maxFTSTopK = ^uint32(0)

const ftsMatchWordDirtyTxnErrMsg = "FTS_MATCH_WORD() cannot be used in a transaction with uncommitted changes"

var ftsScoreType = func() types.FieldType {
	tp := *types.NewFieldType(mysql.TypeFloat)
	tp.SetFlag(mysql.NotNullFlag)
	return tp
}()

func uint32Ptr(v uint32) *uint32 {
	return &v
}

// FullTextIndexPlanVisitor traverses logical plans to find DataSource nodes
// adjacent to FTS_MATCH_WORD() selections or TopN nodes.
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
	if ds, ok := plan.(*logicalop.DataSource); ok {
		return v.onEnterDataSource(v, ds)
	}

	v.parents = append(v.parents, plan)
	children := plan.Children()
	changed := false
	for _, child := range children {
		childChanged, err := v.visit(child)
		if err != nil {
			return false, err
		}
		changed = changed || childChanged
	}
	v.parents = v.parents[:len(v.parents)-1]
	return changed, nil
}

// FullTextIndexResolverWhere resolves FTS_MATCH_WORD() predicates and pushes
// them down to TiFlash table scans. It must run before predicate pushdown.
type FullTextIndexResolverWhere struct{}

// Name returns the name of this optimization rule.
func (*FullTextIndexResolverWhere) Name() string {
	return "fts_resolve_index_where"
}

// Optimize applies full-text index resolution for WHERE predicates.
func (o *FullTextIndexResolverWhere) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{onEnterDataSource: o.onEnterDataSource}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (*FullTextIndexResolverWhere) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	parent0 := v.getParent(0)
	if parent0 == nil {
		return false, nil
	}
	planSelection, ok := parent0.(*logicalop.LogicalSelection)
	if !ok || len(planSelection.Conditions) == 0 {
		return false, nil
	}

	var ftsInfo *expression.FTSInfo
	newConds := make([]expression.Expression, 0, len(planSelection.Conditions)-1)
	for idx, expr := range planSelection.Conditions {
		ftsInfo = expression.InterpretFullTextSearchExpr(expr)
		if ftsInfo != nil {
			newConds = append(newConds, planSelection.Conditions[:idx]...)
			newConds = append(newConds, planSelection.Conditions[idx+1:]...)
			break
		}
	}
	if ftsInfo == nil {
		return false, nil
	}

	matchingIndex := findMatchingFullTextIndex(ds, ftsInfo)
	if matchingIndex == nil {
		return false, plannererrors.ErrWrongUsage.FastGen("Full text search can only be used with a matching fulltext index")
	}

	ds.FtsPushDown = &logicalop.FTSPushDown{
		IndexInfo: matchingIndex,
		QueryInfo: &tipb.FTSQueryInfo{
			QueryType:      tipb.FTSQueryType_FTSQueryTypeNoScore,
			IndexId:        matchingIndex.ID,
			Columns:        []*tipb.ColumnInfo{tidbutil.ColumnToProto(ftsInfo.Column.ToInfo(), false, false)},
			ColumnNames:    []string{ftsInfo.Column.OrigName},
			QueryText:      ftsInfo.Query,
			QueryTokenizer: string(matchingIndex.FullTextInfo.ParserType),
			TopK:           uint32Ptr(maxFTSTopK),
			QueryFunc:      tipb.ScalarFuncSig_FTSMatchWord,
		},
	}
	ds.Columns = append(ds.Columns, &model.ColumnInfo{
		Name:      ast.NewCIStr("_FTS_SCORE"),
		ID:        model.VirtualColFTSScoreID,
		FieldType: ftsScoreType,
	})
	ds.Schema().Append(&expression.Column{
		UniqueID: ds.SCtx().GetSessionVars().AllocPlanColumnID(),
		ID:       model.VirtualColFTSScoreID,
		RetType:  ftsScoreType.Clone(),
		OrigName: "_FTS_SCORE",
	})

	planSelection.Conditions = newConds
	if len(planSelection.Conditions) == 0 {
		removeSelectionNode(v, planSelection)
	}
	return true, nil
}

func findMatchingFullTextIndex(ds *logicalop.DataSource, ftsInfo *expression.FTSInfo) *model.IndexInfo {
	for _, idx := range ds.TableInfo.Indices {
		if idx.FullTextInfo == nil || !idx.IsPublic() || len(idx.Columns) != 1 {
			continue
		}
		if ds.TableInfo.Columns[idx.Columns[0].Offset].ID == ftsInfo.Column.ID {
			return idx
		}
	}
	return nil
}

func removeSelectionNode(v *FullTextIndexPlanVisitor, planSelection *logicalop.LogicalSelection) {
	parent1 := v.getParent(1)
	if parent1 == nil {
		return
	}
	idxToRemove := -1
	for idx, child := range parent1.Children() {
		if child == planSelection {
			idxToRemove = idx
			break
		}
	}
	if idxToRemove < 0 {
		return
	}
	newChildren := make([]base.LogicalPlan, 0, len(parent1.Children())-1+len(planSelection.Children()))
	newChildren = append(newChildren, parent1.Children()[:idxToRemove]...)
	newChildren = append(newChildren, planSelection.Children()...)
	newChildren = append(newChildren, parent1.Children()[idxToRemove+1:]...)
	parent1.SetChildren(newChildren...)
}

// FullTextIndexResolverTopN resolves matching ORDER BY FTS_MATCH_WORD() LIMIT
// expressions after TopN rewrite.
type FullTextIndexResolverTopN struct{}

// Name returns the name of this optimization rule.
func (*FullTextIndexResolverTopN) Name() string {
	return "fts_resolve_index_topn"
}

// Optimize applies TopN optimization for full-text search queries.
func (o *FullTextIndexResolverTopN) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{onEnterDataSource: o.onEnterDataSource}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (*FullTextIndexResolverTopN) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	if ds.FtsPushDown == nil {
		return false, nil
	}

	parent0 := v.getParent(0)
	if parent0 == nil {
		return false, nil
	}
	var planSelection *logicalop.LogicalSelection
	var planTopN *logicalop.LogicalTopN
	if selection, ok := parent0.(*logicalop.LogicalSelection); ok {
		planSelection = selection
		parent1 := v.getParent(1)
		if parent1 == nil {
			return false, nil
		}
		var ok bool
		planTopN, ok = parent1.(*logicalop.LogicalTopN)
		if !ok {
			return false, nil
		}
	} else {
		var ok bool
		planTopN, ok = parent0.(*logicalop.LogicalTopN)
		if !ok {
			return false, nil
		}
	}
	if len(planTopN.ByItems) == 0 {
		return false, nil
	}

	orderByInfo := expression.InterpretFullTextSearchExpr(planTopN.ByItems[0].Expr)
	if orderByInfo == nil {
		return false, nil
	}
	queryInfo := ds.FtsPushDown.QueryInfo
	if len(queryInfo.Columns) == 0 || orderByInfo.Column.ID != queryInfo.Columns[0].ColumnId {
		return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in ORDER BY must match the one in WHERE")
	}
	if orderByInfo.Query != queryInfo.QueryText {
		return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in ORDER BY must match the one in WHERE")
	}

	planTopN.ByItems[0].Expr = ds.Schema().Columns[len(ds.Schema().Columns)-1]
	queryInfo.QueryType = tipb.FTSQueryType_FTSQueryTypeWithScore
	if planSelection == nil && len(ds.PushedDownConds) == 0 && planTopN.ByItems[0].Desc && len(planTopN.ByItems) == 1 {
		topK := planTopN.Offset + planTopN.Count
		if topK > uint64(maxFTSTopK) {
			topK = uint64(maxFTSTopK)
		}
		queryInfo.TopK = uint32Ptr(uint32(topK))
	}
	return true, nil
}

// FullTextIndexResolverProjection resolves matching FTS_MATCH_WORD()
// expressions in SELECT fields to the FTS score column produced by the scan.
type FullTextIndexResolverProjection struct{}

// Name returns the name of this optimization rule.
func (*FullTextIndexResolverProjection) Name() string {
	return "fts_resolve_index_projection"
}

// Optimize applies projection optimization for full-text search queries.
func (o *FullTextIndexResolverProjection) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{onEnterDataSource: o.onEnterDataSource}
	isChanged, err := visitor.visit(plan)
	return plan, isChanged, err
}

func (*FullTextIndexResolverProjection) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	if ds.FtsPushDown == nil {
		return false, nil
	}

	var planSelection *logicalop.LogicalSelection
	var planTopN *logicalop.LogicalTopN
	var planProjection *logicalop.LogicalProjection
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
			return false, nil
		}
		if stopHere {
			break
		}
	}
	if planProjection == nil {
		return false, nil
	}

	queryInfo := ds.FtsPushDown.QueryInfo
	if queryInfo == nil || len(queryInfo.Columns) == 0 {
		return false, nil
	}
	matchedProjections := make([]int, 0, len(planProjection.Exprs))
	for i, expr := range planProjection.Exprs {
		ftsInfo := expression.InterpretFullTextSearchExpr(expr)
		if ftsInfo == nil {
			continue
		}
		if ftsInfo.Column.ID != queryInfo.Columns[0].ColumnId {
			return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		}
		if ftsInfo.Query != queryInfo.QueryText {
			return false, plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		}
		matchedProjections = append(matchedProjections, i)
	}
	if len(matchedProjections) == 0 {
		return false, nil
	}

	scoreColumn := ds.Schema().Columns[len(ds.Schema().Columns)-1]
	for _, idx := range matchedProjections {
		planProjection.Exprs[idx] = scoreColumn
		planProjection.Schema().Columns[idx].RetType = ftsScoreType.Clone()
	}
	queryInfo.QueryType = tipb.FTSQueryType_FTSQueryTypeWithScore
	return true, nil
}

// FullTextIndexResolverRejectRemaining rejects unsupported FTS_MATCH_WORD()
// patterns left after the pushdown rewrite rules.
type FullTextIndexResolverRejectRemaining struct{}

// Name returns the name of this optimization rule.
func (*FullTextIndexResolverRejectRemaining) Name() string {
	return "fts_resolve_reject_remaining"
}

// Optimize validates remaining full-text search usage.
func (o *FullTextIndexResolverRejectRemaining) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.FTSFunctionIsUsed {
		return plan, false, nil
	}
	if err := o.visit(plan); err != nil {
		return plan, false, err
	}
	return plan, false, nil
}

func (o *FullTextIndexResolverRejectRemaining) visit(plan base.LogicalPlan) error {
	switch p := plan.(type) {
	case *logicalop.LogicalProjection:
		for _, expr := range p.Exprs {
			if expression.InterpretFullTextSearchExpr(expr) != nil {
				return plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE. A valid example: SELECT FTS_MATCH_WORD(...) FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
			if expression.ContainsFullTextSearchFn(expr) {
				return plannererrors.ErrWrongUsage.FastGen("'FTS_MATCH_WORD()' in SELECT must not be wrapped in expressions. A valid example: SELECT FTS_MATCH_WORD(...) FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.LogicalUnionScan:
		if containsFullTextSearchFn(p.Conditions) {
			return plannererrors.ErrWrongUsage.FastGen(ftsMatchWordDirtyTxnErrMsg)
		}
	case *logicalop.DataSource:
		for _, item := range p.PushedDownConds {
			if expression.ContainsFullTextSearchFn(item) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.LogicalSelection:
		if len(p.Children()) == 1 {
			if _, ok := p.Children()[0].(*logicalop.LogicalUnionScan); ok && containsFullTextSearchFn(p.Conditions) {
				return plannererrors.ErrWrongUsage.FastGen(ftsMatchWordDirtyTxnErrMsg)
			}
		}
		for _, cond := range p.Conditions {
			if expression.ContainsFullTextSearchFn(cond) {
				return plannererrors.ErrWrongUsage.FastGen("Currently 'FTS_MATCH_WORD()' must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...)")
			}
		}
	case *logicalop.LogicalTopN:
		for i, item := range p.ByItems {
			if expression.ContainsFullTextSearchFn(item.Expr) {
				if i > 0 {
					return plannererrors.ErrWrongUsage.FastGen("FTS_MATCH_WORD() must be used as the first item in ORDER BY. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT ..")
				}
				return plannererrors.ErrWrongUsage.FastGen("Unsupported 'FTS_MATCH_WORD()' usage. It must be used with a WHERE clause and must be used alone. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT ..")
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

func containsFullTextSearchFn(exprs []expression.Expression) bool {
	for _, expr := range exprs {
		if expression.ContainsFullTextSearchFn(expr) {
			return true
		}
	}
	return false
}
