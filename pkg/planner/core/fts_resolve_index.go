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
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tipb/go-tipb"
)

// FullTextIndexPlanVisitor traverses logical plans and resolves a MATCH ...
// AGAINST predicate adjacent to a table data source.
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
	changed := false
	for _, child := range plan.Children() {
		childChanged, err := v.visit(child)
		if err != nil {
			return false, err
		}
		changed = changed || childChanged
	}
	v.parents = v.parents[:len(v.parents)-1]
	return changed, nil
}

// FullTextIndexResolverWhere resolves native Boolean MATCH predicates before
// ordinary predicate pushdown removes the table/index context they need.
type FullTextIndexResolverWhere struct{}

func (*FullTextIndexResolverWhere) Name() string {
	return "fts_resolve_index_where"
}

func (o *FullTextIndexResolverWhere) Optimize(_ context.Context, plan base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if !plan.SCtx().GetSessionVars().StmtCtx.AlternativeLogicalPlanHasPredicateMatch {
		return plan, false, nil
	}
	visitor := &FullTextIndexPlanVisitor{onEnterDataSource: o.onEnterDataSource}
	changed, err := visitor.visit(plan)
	return plan, changed, err
}

func (*FullTextIndexResolverWhere) onEnterDataSource(v *FullTextIndexPlanVisitor, ds *logicalop.DataSource) (bool, error) {
	selection, ok := v.getParent(0).(*logicalop.LogicalSelection)
	if !ok || len(selection.Conditions) == 0 {
		return false, nil
	}

	for i, condition := range selection.Conditions {
		ftsInfo := expression.InterpretFullTextSearchExpr(condition)
		if ftsInfo == nil {
			continue
		}

		if sf, ok := condition.(*expression.ScalarFunction); ok {
			if _, local := expression.FTSMysqlMatchAgainstLocalEvalInfo(sf); local {
				// Keep the planner-selected TiDB fallback as a residual predicate.
				return false, nil
			}
		}

		matchingIndex := findMatchingFullTextIndex(ds, ftsInfo)
		if matchingIndex == nil {
			return false, plannererrors.ErrWrongUsage.FastGen("Boolean MATCH requires a matching public FULLTEXT index")
		}

		analyzerConfig, err := fulltext.AnalyzerConfigFromSessionVars(
			ds.SCtx().GetSessionVars(),
			matchingIndex.FullTextInfo.ParserType,
		)
		if err != nil {
			return false, plannererrors.ErrWrongUsage.FastGen("cannot configure BOOLEAN MODE full-text analyzer: %s", err)
		}
		booleanQuery, err := expression.BuildFTSBooleanQueryWithAnalyzerConfig(ftsInfo.Query, analyzerConfig)
		if err != nil {
			return false, plannererrors.ErrWrongUsage.FastGen("unsupported BOOLEAN MODE full-text query: %s", err)
		}

		columns := make([]*tipb.ColumnInfo, 0, len(ftsInfo.Columns))
		columnNames := make([]string, 0, len(ftsInfo.Columns))
		for _, column := range ftsInfo.Columns {
			columns = append(columns, tidbutil.ColumnToProto(column.ToInfo(), false, false))
			columnNames = append(columnNames, column.OrigName)
		}
		topK := ^uint32(0)
		ds.FtsPushDown = &logicalop.FTSPushDown{
			IndexInfo: matchingIndex,
			QueryInfo: &tipb.FTSQueryInfo{
				QueryType:      tipb.FTSQueryType_FTSQueryTypeNoScore,
				IndexId:        matchingIndex.ID,
				QueryText:      ftsInfo.Query,
				QueryTokenizer: string(matchingIndex.FullTextInfo.ParserType),
				TopK:           &topK,
				QueryFunc:      tipb.ScalarFuncSig_FTSMatchExpression,
				BooleanQuery:   booleanQuery,
				Columns:        columns,
				ColumnNames:    columnNames,
			},
		}

		selection.Conditions = append(selection.Conditions[:i], selection.Conditions[i+1:]...)
		if len(selection.Conditions) == 0 {
			removeSelectionNode(v, selection)
		}
		return true, nil
	}
	return false, nil
}

func findMatchingFullTextIndex(ds *logicalop.DataSource, ftsInfo *expression.FTSInfo) *model.IndexInfo {
	if ftsInfo == nil || !ftsInfo.IsMatchAgainst || ds.TableInfo == nil {
		return nil
	}
	columnNames := make([]pmodel.CIStr, 0, len(ftsInfo.Columns))
	for _, column := range ftsInfo.Columns {
		columnInfo := ds.TableInfo.FindColumnByID(column.ID)
		if columnInfo == nil {
			return nil
		}
		columnNames = append(columnNames, columnInfo.Name)
	}
	return publicFTSIndexOnColumns(ds.TableInfo, columnNames, true)
}

// publicFTSIndexOnColumns requires one public composite FULLTEXT index whose
// ordered columns exactly match the MATCH column list; separate single-column
// indexes are not an equivalent substitute.
func publicFTSIndexOnColumns(tblInfo *model.TableInfo, columnNames []pmodel.CIStr, nativeParserOnly bool) *model.IndexInfo {
	if tblInfo == nil || len(columnNames) == 0 {
		return nil
	}
	for _, index := range tblInfo.Indices {
		if index.FullTextInfo == nil || !index.IsPublic() || len(index.Columns) != len(columnNames) {
			continue
		}
		if nativeParserOnly && !isNativeFTSParser(index.FullTextInfo.ParserType) {
			continue
		}
		matched := true
		for i, column := range index.Columns {
			if column.Name.L != columnNames[i].L {
				matched = false
				break
			}
		}
		if matched {
			return index
		}
	}
	return nil
}

func isNativeFTSParser(parserType model.FullTextParserType) bool {
	return parserType == model.FullTextParserTypeStandardV1 || parserType == model.FullTextParserTypeNgramV1
}

func removeSelectionNode(v *FullTextIndexPlanVisitor, selection *logicalop.LogicalSelection) {
	parent := v.getParent(1)
	if parent == nil {
		return
	}
	childIndex := -1
	for i, child := range parent.Children() {
		if child == selection {
			childIndex = i
			break
		}
	}
	if childIndex < 0 {
		return
	}
	children := make([]base.LogicalPlan, 0, len(parent.Children())-1+len(selection.Children()))
	children = append(children, parent.Children()[:childIndex]...)
	children = append(children, selection.Children()...)
	children = append(children, parent.Children()[childIndex+1:]...)
	parent.SetChildren(children...)
}
