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

package logicalop

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestResolveFTSWherePredicateIsDataSourceLocal(t *testing.T) {
	setupStarterFTSTest(t)
	store, err := mockstore.NewMockStore(mockstore.WithMockTiFlash(1))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	newDataSource := func() (*DataSource, *util.AccessPath, *model.ColumnInfo) {
		ctx := mock.NewContext()
		ctx.Store = store
		ctx.GetSessionVars().StmtCtx.FTSFunctionIsUsed = true
		ctx.GetSessionVars().IsolationReadEngines = map[kv.StoreType]struct{}{
			kv.TiKV:    {},
			kv.TiFlash: {},
		}
		columnInfo := &model.ColumnInfo{
			ID:        11,
			Name:      ast.NewCIStr("title"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		indexInfo := &model.IndexInfo{
			ID:           21,
			Name:         ast.NewCIStr("ft_title"),
			State:        model.StatePublic,
			Columns:      []*model.IndexColumn{{Name: columnInfo.Name, Offset: 0}},
			FullTextInfo: &ast.FullTextIndexInfo{ParserType: ast.FullTextParserTypeStandardV1},
		}
		tableInfo := &model.TableInfo{
			ID:             31,
			Name:           ast.NewCIStr("t"),
			Columns:        []*model.ColumnInfo{columnInfo},
			Indices:        []*model.IndexInfo{indexInfo},
			TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1, Available: true},
		}
		column := &expression.Column{
			UniqueID: 41,
			ID:       columnInfo.ID,
			RetType:  columnInfo.FieldType.Clone(),
			OrigName: "test.t.title",
		}
		ftsExpr := expression.NewFunctionInternal(
			ctx.GetExprCtx(),
			ast.FTSMatchWord,
			types.NewFieldType(mysql.TypeDouble),
			&expression.Constant{Value: types.NewStringDatum("hello"), RetType: types.NewFieldType(mysql.TypeVarchar)},
			column,
		)
		ftsPath := &util.AccessPath{
			Index:            indexInfo,
			StoreType:        kv.TiFlash,
			IsIntHandlePath:  true,
			Forced:           true,
			ForceKeepOrder:   true,
			ForceNoKeepOrder: true,
		}
		ds := DataSource{
			TableInfo:           tableInfo,
			PhysicalTableID:     tableInfo.ID,
			AllConds:            []expression.Expression{ftsExpr},
			PushedDownConds:     []expression.Expression{ftsExpr},
			PossibleAccessPaths: []*util.AccessPath{ftsPath},
			Columns:             []*model.ColumnInfo{columnInfo},
		}.Init(ctx, 0)
		ds.SetSchema(expression.NewSchema(column))
		return ds, ftsPath, columnInfo
	}
	newFTSExpr := func(ds *DataSource, query string) expression.Expression {
		return expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.FTSMatchWord,
			types.NewFieldType(mysql.TypeDouble),
			&expression.Constant{Value: types.NewStringDatum(query), RetType: types.NewFieldType(mysql.TypeVarchar)},
			ds.Schema().Columns[0],
		)
	}
	newIndex := func(columnInfo *model.ColumnInfo, id int64, name string, parser ast.FullTextAnalyzerType) *model.IndexInfo {
		return &model.IndexInfo{
			ID:           id,
			Name:         ast.NewCIStr(name),
			State:        model.StatePublic,
			Columns:      []*model.IndexColumn{{Name: columnInfo.Name, Offset: 0}},
			FullTextInfo: &ast.FullTextIndexInfo{ParserType: parser},
		}
	}
	newPath := func(indexInfo *model.IndexInfo) *util.AccessPath {
		return &util.AccessPath{
			Index:           indexInfo,
			StoreType:       kv.TiFlash,
			IsIntHandlePath: true,
		}
	}
	requireExpressionSliceIdentity := func(t *testing.T, expected, actual []expression.Expression) {
		t.Helper()
		require.Len(t, actual, len(expected))
		for i := range expected {
			require.True(t, expected[i] == actual[i], "expression %d changed identity", i)
		}
	}
	decodeQueryClause := func(t *testing.T, info *tipb.FTSQueryInfo) expression.QueryClause {
		t.Helper()
		require.Len(t, info.MatchExpr, 1)
		require.Equal(t, tipb.ExprType_ScalarFunc, info.MatchExpr[0].Tp)
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchWord, info.MatchExpr[0].Sig)
		clause, err := expression.DecodeFTSQueryClause(info.MatchExpr[0])
		require.NoError(t, err)
		return clause
	}
	requireLocalNoOp := func(t *testing.T, ds *DataSource) {
		t.Helper()
		allConds := append([]expression.Expression(nil), ds.AllConds...)
		pushedDownConds := append([]expression.Expression(nil), ds.PushedDownConds...)
		paths := append([]*util.AccessPath(nil), ds.PossibleAccessPaths...)
		pathSnapshots := make([]util.AccessPath, len(paths))
		for i, path := range paths {
			pathSnapshots[i] = *path
		}
		columns := append([]*model.ColumnInfo(nil), ds.Columns...)
		schemaColumns := append([]*expression.Column(nil), ds.Schema().Columns...)
		planColumnID := ds.SCtx().GetSessionVars().PlanColumnID.Load()
		warningCount := len(ds.SCtx().GetSessionVars().StmtCtx.GetWarnings())

		require.NoError(t, ds.resolveFTSWherePredicate())
		requireExpressionSliceIdentity(t, allConds, ds.AllConds)
		requireExpressionSliceIdentity(t, pushedDownConds, ds.PushedDownConds)
		require.Len(t, ds.PossibleAccessPaths, len(paths))
		for i, path := range paths {
			require.Same(t, path, ds.PossibleAccessPaths[i])
			require.Equal(t, pathSnapshots[i], *ds.PossibleAccessPaths[i])
		}
		require.Equal(t, columns, ds.Columns)
		require.Equal(t, schemaColumns, ds.Schema().Columns)
		require.Nil(t, ds.FtsPushDown)
		require.Equal(t, planColumnID, ds.SCtx().GetSessionVars().PlanColumnID.Load())
		require.Len(t, ds.SCtx().GetSessionVars().StmtCtx.GetWarnings(), warningCount)
	}
	newMultiColumnFTSDataSource := func(titleParser, bodyParser string) (*DataSource, *model.IndexInfo, *model.ColumnInfo, *model.ColumnInfo) {
		ctx := mock.NewContext()
		ctx.Store = store
		ctx.GetSessionVars().StmtCtx.FTSFunctionIsUsed = true
		ctx.GetSessionVars().IsolationReadEngines = map[kv.StoreType]struct{}{
			kv.TiKV:    {},
			kv.TiFlash: {},
		}
		titleInfo := &model.ColumnInfo{
			ID:        11,
			Name:      ast.NewCIStr("title"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		bodyInfo := &model.ColumnInfo{
			ID:        12,
			Name:      ast.NewCIStr("body"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		pathInfo := &model.ColumnInfo{
			ID:        13,
			Name:      ast.NewCIStr("path"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		indexInfo := &model.IndexInfo{
			ID:    21,
			Name:  ast.NewCIStr("ft_text"),
			State: model.StatePublic,
			Columns: []*model.IndexColumn{
				{Name: titleInfo.Name, Offset: 0},
				{Name: bodyInfo.Name, Offset: 1},
				{Name: pathInfo.Name, Offset: 2},
			},
			FullTextInfo: &ast.FullTextIndexInfo{
				IndexFields: []ast.FullTextIndexFieldInfo{
					{
						ColumnID: titleInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypeStandardV1,
							EnableBM25:   true,
						},
					},
					{
						ColumnID: bodyInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypeExactV1,
							EnableBM25:   true,
						},
					},
					{
						ColumnID: bodyInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypeNgramV1,
							EnableBM25:   true,
						},
					},
					{
						ColumnID: pathInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypePathHierarchyV1,
							PathHierarchy: &ast.FullTextPathHierarchyOption{
								Delimiter: "/",
							},
						},
					},
				},
			},
		}
		tableInfo := &model.TableInfo{
			ID:             31,
			Name:           ast.NewCIStr("t"),
			Columns:        []*model.ColumnInfo{titleInfo, bodyInfo, pathInfo},
			Indices:        []*model.IndexInfo{indexInfo},
			TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1, Available: true},
		}
		titleColumn := &expression.Column{
			UniqueID: 41,
			ID:       titleInfo.ID,
			RetType:  titleInfo.FieldType.Clone(),
			OrigName: "test.t.title",
		}
		bodyColumn := &expression.Column{
			UniqueID: 42,
			ID:       bodyInfo.ID,
			RetType:  bodyInfo.FieldType.Clone(),
			OrigName: "test.t.body",
		}
		pathColumn := &expression.Column{
			UniqueID: 43,
			ID:       pathInfo.ID,
			RetType:  pathInfo.FieldType.Clone(),
			OrigName: "test.t.path",
		}
		ftsExpr := expression.NewFunctionInternal(
			ctx.GetExprCtx(),
			ast.FTSMatchWord,
			types.NewFieldType(mysql.TypeDouble),
			&expression.Constant{Value: types.NewStringDatum("hello"), RetType: types.NewFieldType(mysql.TypeVarchar)},
			titleColumn,
			bodyColumn,
		).(*expression.ScalarFunction)
		expression.SetFTSMatchWordOptions(ftsExpr, &expression.FTSMatchWordOptions{
			Fields: []expression.FTSMatchWordFieldOption{
				{ParserName: titleParser},
				{ParserName: bodyParser},
			},
		})
		ftsPath := &util.AccessPath{
			Index:           indexInfo,
			StoreType:       kv.TiFlash,
			IsIntHandlePath: true,
		}
		ds := DataSource{
			TableInfo:           tableInfo,
			PhysicalTableID:     tableInfo.ID,
			AllConds:            []expression.Expression{ftsExpr},
			PushedDownConds:     []expression.Expression{ftsExpr},
			PossibleAccessPaths: []*util.AccessPath{ftsPath},
			Columns:             []*model.ColumnInfo{titleInfo, bodyInfo, pathInfo},
		}.Init(ctx, 0)
		ds.SetSchema(expression.NewSchema(titleColumn, bodyColumn, pathColumn))
		return ds, indexInfo, titleInfo, bodyInfo
	}

	t.Run("bare direct predicate is consumed", func(t *testing.T) {
		ds, ftsPath, _ := newDataSource()
		planColumnID := ds.SCtx().GetSessionVars().PlanColumnID.Load()

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
		require.NotNil(t, ds.FtsPushDown)
		require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, ds.FtsPushDown.QueryType)
		require.Equal(t, expression.QueryClause{
			Version:   expression.FTSQueryClauseVersion,
			ScoreMode: expression.FtsScoreModeInherit,
			Query:     "hello",
			Fields: []expression.QueryField{{
				ColID:      11,
				IndexField: string(ast.FullTextParserTypeStandardV1),
			}},
		}, decodeQueryClause(t, ds.FtsPushDown))
		require.Empty(t, ds.FtsPushDown.QueryText)
		require.Empty(t, ds.FtsPushDown.QueryTokenizer)
		require.Empty(t, ds.FtsPushDown.Columns)
		require.Empty(t, ds.FtsPushDown.ColumnNames)
		require.Equal(t, ^uint32(0), *ds.FtsPushDown.TopK)
		require.Equal(t, []*util.AccessPath{ftsPath}, ds.PossibleAccessPaths)
		require.True(t, ftsPath.Forced)
		require.False(t, ftsPath.ForceKeepOrder)
		require.True(t, ftsPath.ForceNoKeepOrder)
		warnings := ds.SCtx().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, warnings, 1)
		require.ErrorContains(t, warnings[0].Err, "ORDER_INDEX is inapplicable")
		require.Equal(t, planColumnID+1, ds.SCtx().GetSessionVars().PlanColumnID.Load())
	})

	t.Run("rejects a scored pure-negative query tree", func(t *testing.T) {
		ds, path, _ := newDataSource()
		predicate := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), ds.AllConds[0],
		)
		ds.AllConds = []expression.Expression{predicate}
		ds.PushedDownConds = []expression.Expression{predicate}
		pathSnapshot := *path
		planColumnID := ds.SCtx().GetSessionVars().PlanColumnID.Load()

		err := ds.resolveFTSWherePredicate()
		require.ErrorContains(t, err, "scored pure-negative FTS match expression is not supported")
		requireExpressionSliceIdentity(t, []expression.Expression{predicate}, ds.AllConds)
		requireExpressionSliceIdentity(t, []expression.Expression{predicate}, ds.PushedDownConds)
		require.Equal(t, pathSnapshot, *path)
		require.Nil(t, ds.FtsPushDown)
		require.Len(t, ds.Columns, 1)
		require.Len(t, ds.Schema().Columns, 1)
		require.Equal(t, planColumnID, ds.SCtx().GetSessionVars().PlanColumnID.Load())
	})

	t.Run("multi-column index uses the bound column analyzer", func(t *testing.T) {
		ctx := mock.NewContext()
		ctx.Store = store
		ctx.GetSessionVars().StmtCtx.FTSFunctionIsUsed = true
		ctx.GetSessionVars().IsolationReadEngines = map[kv.StoreType]struct{}{
			kv.TiKV:    {},
			kv.TiFlash: {},
		}
		titleInfo := &model.ColumnInfo{
			ID:        11,
			Name:      ast.NewCIStr("title"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		bodyInfo := &model.ColumnInfo{
			ID:        12,
			Name:      ast.NewCIStr("body"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		indexInfo := &model.IndexInfo{
			ID:    21,
			Name:  ast.NewCIStr("ft_text"),
			State: model.StatePublic,
			Columns: []*model.IndexColumn{
				{Name: titleInfo.Name, Offset: 0},
				{Name: bodyInfo.Name, Offset: 1},
			},
			FullTextInfo: &ast.FullTextIndexInfo{
				IndexFields: []ast.FullTextIndexFieldInfo{
					{
						ColumnID: titleInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypeStandardV1,
							EnableBM25:   true,
						},
					},
					{
						ColumnID: bodyInfo.ID,
						IndexFieldOption: ast.FullTextIndexFieldOption{
							AnalyzerType: ast.FullTextParserTypeNgramV1,
							EnableBM25:   true,
						},
					},
				},
			},
		}
		tableInfo := &model.TableInfo{
			ID:             31,
			Name:           ast.NewCIStr("t"),
			Columns:        []*model.ColumnInfo{titleInfo, bodyInfo},
			Indices:        []*model.IndexInfo{indexInfo},
			TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1, Available: true},
		}
		titleColumn := &expression.Column{
			UniqueID: 41,
			ID:       titleInfo.ID,
			RetType:  titleInfo.FieldType.Clone(),
			OrigName: "test.t.title",
		}
		bodyColumn := &expression.Column{
			UniqueID: 42,
			ID:       bodyInfo.ID,
			RetType:  bodyInfo.FieldType.Clone(),
			OrigName: "test.t.body",
		}
		ftsExpr := expression.NewFunctionInternal(
			ctx.GetExprCtx(),
			ast.FTSMatchWord,
			types.NewFieldType(mysql.TypeDouble),
			&expression.Constant{Value: types.NewStringDatum("body words"), RetType: types.NewFieldType(mysql.TypeVarchar)},
			bodyColumn,
		)
		ftsPath := &util.AccessPath{
			Index:           indexInfo,
			StoreType:       kv.TiFlash,
			IsIntHandlePath: true,
			ForceKeepOrder:  true,
		}
		ds := DataSource{
			TableInfo:           tableInfo,
			PhysicalTableID:     tableInfo.ID,
			AllConds:            []expression.Expression{ftsExpr},
			PushedDownConds:     []expression.Expression{ftsExpr},
			PossibleAccessPaths: []*util.AccessPath{ftsPath},
			Columns:             []*model.ColumnInfo{titleInfo, bodyInfo},
		}.Init(ctx, 0)
		ds.SetSchema(expression.NewSchema(titleColumn, bodyColumn))

		indexSnapshot := indexInfo.Clone()
		fullTextInfoSnapshot := *indexInfo.FullTextInfo
		fullTextInfoSnapshot.IndexFields = append(
			[]ast.FullTextIndexFieldInfo(nil), indexInfo.FullTextInfo.IndexFields...,
		)
		indexSnapshot.FullTextInfo = &fullTextInfoSnapshot
		pathSnapshot := *ftsPath
		pathSnapshot.ForceKeepOrder = false

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
		require.NotNil(t, ds.FtsPushDown)
		require.Equal(t, indexInfo.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, expression.QueryClause{
			Version:   expression.FTSQueryClauseVersion,
			ScoreMode: expression.FtsScoreModeInherit,
			Query:     "body words",
			Fields: []expression.QueryField{{
				ColID:      bodyInfo.ID,
				IndexField: string(ast.FullTextParserTypeNgramV1),
			}},
		}, decodeQueryClause(t, ds.FtsPushDown))
		require.Empty(t, ds.FtsPushDown.QueryText)
		require.Empty(t, ds.FtsPushDown.QueryTokenizer)
		require.Empty(t, ds.FtsPushDown.Columns)
		require.Empty(t, ds.FtsPushDown.ColumnNames)
		require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, ds.FtsPushDown.QueryType)
		require.Len(t, ds.PossibleAccessPaths, 1)
		require.Same(t, ftsPath, ds.PossibleAccessPaths[0])
		require.Same(t, indexInfo, ftsPath.Index)
		require.Equal(t, pathSnapshot, *ftsPath)
		require.Equal(t, indexSnapshot, indexInfo)
		require.Equal(t, fullTextInfoSnapshot.IndexFields, indexInfo.FullTextInfo.IndexFields)
		require.Equal(t, ast.FullTextParserTypeStandardV1, indexInfo.FullTextInfo.AnalyzerTypeForColumn(titleInfo.ID))
		require.Equal(t, ast.FullTextParserTypeNgramV1, indexInfo.FullTextInfo.AnalyzerTypeForColumn(bodyInfo.ID))
	})

	t.Run("match word resolves different analyzers for multiple columns", func(t *testing.T) {
		ds, indexInfo, titleInfo, bodyInfo := newMultiColumnFTSDataSource("", "NGRAM")

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
		require.NotNil(t, ds.FtsPushDown)
		require.Equal(t, indexInfo.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchWord, ds.FtsPushDown.QueryFunc)
		require.Equal(t, expression.QueryClause{
			Version:   expression.FTSQueryClauseVersion,
			ScoreMode: expression.FtsScoreModeInherit,
			Query:     "hello",
			Fields: []expression.QueryField{
				{ColID: titleInfo.ID, IndexField: string(ast.FullTextParserTypeStandardV1)},
				{ColID: bodyInfo.ID, IndexField: string(ast.FullTextParserTypeNgramV1)},
			},
		}, decodeQueryClause(t, ds.FtsPushDown))
		require.Empty(t, ds.FtsPushDown.QueryText)
		require.Empty(t, ds.FtsPushDown.QueryTokenizer)
		require.Empty(t, ds.FtsPushDown.Columns)
		require.Empty(t, ds.FtsPushDown.ColumnNames)
	})

	t.Run("match word rejects an explicit exact analyzer", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("STANDARD", "EXACT")

		require.ErrorContains(t, ds.resolveFTSWherePredicate(), "does not support fields using the EXACT parser")
		require.NotEmpty(t, ds.AllConds)
		require.NotEmpty(t, ds.PushedDownConds)
		require.Nil(t, ds.FtsPushDown)
	})

	t.Run("compound full-text query serializes the rebuilt expression tree", func(t *testing.T) {
		ds, indexInfo, titleInfo, bodyInfo := newMultiColumnFTSDataSource("", "EXACT")
		titleColumn := ds.Schema().Columns[0]
		bodyColumn := ds.Schema().Columns[1]
		matchWord := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.FTSMatchWord,
			types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("hello"),
			titleColumn,
		)
		exactMatch := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.EQ,
			types.NewFieldType(mysql.TypeTiny),
			bodyColumn,
			expression.NewStrConst("document-id"),
		)
		compound := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.LogicOr,
			types.NewFieldType(mysql.TypeTiny),
			matchWord,
			exactMatch,
		)
		ds.AllConds = []expression.Expression{compound}
		ds.PushedDownConds = []expression.Expression{compound}

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
		require.NotNil(t, ds.FtsPushDown)
		require.Equal(t, indexInfo.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchExpression, ds.FtsPushDown.QueryFunc)
		require.Len(t, ds.FtsPushDown.MatchExpr, 1)

		root := ds.FtsPushDown.MatchExpr[0]
		require.Equal(t, tipb.ScalarFuncSig_LogicalOr, root.Sig)
		var rootMetadata struct {
			Version uint `json:"version"`
		}
		require.NoError(t, json.Unmarshal(root.Val, &rootMetadata))
		require.Equal(t, expression.FTSQueryClauseVersion, rootMetadata.Version)
		require.Len(t, root.Children, 2)
		matchWordPB := root.Children[0]
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchWord, matchWordPB.Sig)
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchWord, root.Children[1].Sig)
		clause, err := expression.DecodeFTSQueryClause(*matchWordPB)
		require.NoError(t, err)
		require.Equal(t, expression.QueryClause{
			Version:   expression.FTSQueryClauseVersion,
			ScoreMode: expression.FtsScoreModeInherit,
			Query:     "hello",
			Fields: []expression.QueryField{{
				ColID:      titleInfo.ID,
				IndexField: string(ast.FullTextParserTypeStandardV1),
			}},
		}, clause)
		exactClause, err := expression.DecodeFTSQueryClause(*root.Children[1])
		require.NoError(t, err)
		require.Equal(t, expression.QueryClause{
			Version:   expression.FTSQueryClauseVersion,
			ScoreMode: expression.FtsScoreModeInherit,
			Query:     "document-id",
			Fields: []expression.QueryField{{
				ColID:      bodyInfo.ID,
				IndexField: string(ast.FullTextParserTypeExactV1),
			}},
		}, exactClause)
		require.Equal(t, bodyInfo.ID, bodyColumn.ID)
		require.Nil(t, expression.GetFTSMatchWordOptions(matchWord.(*expression.ScalarFunction)))
	})

	t.Run("absorbs covered CNF predicates into the match tree", func(t *testing.T) {
		ds, indexInfo, titleInfo, bodyInfo := newMultiColumnFTSDataSource("", "NGRAM")
		titleColumn := ds.Schema().Columns[0]
		bodyColumn := ds.Schema().Columns[1]
		pathColumn := ds.Schema().Columns[2]
		newMatch := func(query string, column *expression.Column, parser string) *expression.ScalarFunction {
			match := expression.NewFunctionInternal(
				ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
				expression.NewStrConst(query), column,
			).(*expression.ScalarFunction)
			if parser != "" {
				expression.SetFTSMatchWordOptions(match, &expression.FTSMatchWordOptions{
					Fields: []expression.FTSMatchWordFieldOption{{ParserName: parser}},
				})
			}
			return match
		}
		anchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.LogicOr, types.NewFieldType(mysql.TypeTiny),
			newMatch("foo", titleColumn, ""),
			newMatch("bar", bodyColumn, "NGRAM"),
		)
		exactFilter := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
			bodyColumn, expression.NewStrConst("document-id"),
		)
		pathFilter := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.Like, types.NewFieldType(mysql.TypeTiny),
			pathColumn, expression.NewStrConst("/path/to/%"), expression.NewInt64Const('\\'),
		)
		ds.AllConds = []expression.Expression{anchor, pathFilter, exactFilter}
		ds.PushedDownConds = []expression.Expression{anchor, pathFilter, exactFilter}

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
		require.Empty(t, ds.PossibleAccessPaths[0].AccessConds)
		require.Equal(t, indexInfo.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, tipb.ScalarFuncSig_FTSMatchExpression, ds.FtsPushDown.QueryFunc)

		clauses := make(map[string]expression.QueryClause)
		queries := make([]string, 0, 4)
		var visit func(tipb.Expr)
		visit = func(expr tipb.Expr) {
			if expr.Sig == tipb.ScalarFuncSig_FTSMatchWord {
				clause, err := expression.DecodeFTSQueryClause(expr)
				require.NoError(t, err)
				query := fmt.Sprint(clause.Query)
				queries = append(queries, query)
				clauses[query] = clause
			}
			for _, child := range expr.Children {
				visit(*child)
			}
		}
		visit(ds.FtsPushDown.MatchExpr[0])
		require.Equal(t, []string{"foo", "bar", "/path/to/", "document-id"}, queries)
		require.Len(t, clauses, 4)
		require.Equal(t, expression.FtsScoreModeInherit, clauses["foo"].ScoreMode)
		require.Equal(t, string(ast.FullTextParserTypeStandardV1), clauses["foo"].Fields[0].IndexField)
		require.Equal(t, expression.FtsScoreModeInherit, clauses["bar"].ScoreMode)
		require.Equal(t, string(ast.FullTextParserTypeNgramV1), clauses["bar"].Fields[0].IndexField)
		require.Equal(t, expression.FtsScoreModeInherit, clauses["document-id"].ScoreMode)
		require.Equal(t, string(ast.FullTextParserTypeExactV1), clauses["document-id"].Fields[0].IndexField)
		require.Equal(t, expression.FtsScoreModeInherit, clauses["/path/to/"].ScoreMode)
		require.Equal(t, string(ast.FullTextParserTypePathHierarchyV1), clauses["/path/to/"].Fields[0].IndexField)
		require.Equal(t, titleInfo.ID, clauses["foo"].Fields[0].ColID)
		require.Equal(t, bodyInfo.ID, clauses["document-id"].Fields[0].ColID)

		rebuiltAnchor, ok := CheckExprCoveredByColumnarFTSAndRebuild(
			ds.SCtx().GetExprCtx(), anchor, ds.TableInfo, indexInfo,
		)
		require.True(t, ok)
		anchorPB, err := expression.FTSQueryExpressionToPB(
			ds.SCtx().GetExprCtx().GetEvalCtx(), ds.SCtx().GetClient(), rebuiltAnchor,
		)
		require.NoError(t, err)
		require.True(t, expression.FTSQueryPBMatchesScoringExpr(&ds.FtsPushDown.MatchExpr[0], anchorPB))
		var filterMetadata struct {
			Version   uint   `json:"version"`
			ScoreMode string `json:"score_mode"`
		}
		require.NoError(t, json.Unmarshal(ds.FtsPushDown.MatchExpr[0].Children[1].Val, &filterMetadata))
		require.Equal(t, expression.FTSQueryClauseVersion, filterMetadata.Version)
		require.Equal(t, expression.FtsScoreModeFilter, filterMetadata.ScoreMode)

		mismatchedAnchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.LogicOr, types.NewFieldType(mysql.TypeTiny),
			newMatch("different", titleColumn, ""),
			newMatch("bar", bodyColumn, "NGRAM"),
		)
		rebuiltMismatch, ok := CheckExprCoveredByColumnarFTSAndRebuild(
			ds.SCtx().GetExprCtx(), mismatchedAnchor, ds.TableInfo, indexInfo,
		)
		require.True(t, ok)
		mismatchPB, err := expression.FTSQueryExpressionToPB(
			ds.SCtx().GetExprCtx().GetEvalCtx(), ds.SCtx().GetClient(), rebuiltMismatch,
		)
		require.NoError(t, err)
		require.False(t, expression.FTSQueryPBMatchesScoringExpr(&ds.FtsPushDown.MatchExpr[0], mismatchPB))
	})

	t.Run("keeps compound absorbed predicates in inherited score mode", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("", "NGRAM")
		anchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("foo"), ds.Schema().Columns[0],
		)
		leftExact := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[1], expression.NewStrConst("left"),
		)
		rightExact := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[1], expression.NewStrConst("right"),
		)
		absorbed := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.LogicOr, types.NewFieldType(mysql.TypeTiny),
			leftExact, rightExact,
		)
		ds.AllConds = []expression.Expression{anchor, absorbed}
		ds.PushedDownConds = []expression.Expression{anchor, absorbed}

		require.NoError(t, ds.resolveFTSWherePredicate())
		root := ds.FtsPushDown.MatchExpr[0]
		require.Equal(t, tipb.ScalarFuncSig_LogicalAnd, root.Sig)
		require.Len(t, root.Children, 2)
		absorbedRoot := root.Children[1]
		require.Equal(t, tipb.ScalarFuncSig_LogicalOr, absorbedRoot.Sig)
		require.Empty(t, absorbedRoot.Val)
		for _, child := range absorbedRoot.Children {
			clause, err := expression.DecodeFTSQueryClause(*child)
			require.NoError(t, err)
			require.Equal(t, expression.FtsScoreModeInherit, clause.ScoreMode)
		}
	})

	t.Run("retains predicates not covered by the selected FTS index", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("", "NGRAM")
		anchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("foo"), ds.Schema().Columns[0],
		)
		covered := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.Like, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[2], expression.NewStrConst("/path/to/%"), expression.NewInt64Const('\\'),
		)
		residual := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[0], expression.NewStrConst("not-exact"),
		)
		ds.AllConds = []expression.Expression{anchor, covered, residual}
		ds.PushedDownConds = []expression.Expression{anchor, covered, residual}

		require.NoError(t, ds.resolveFTSWherePredicate())
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.AllConds)
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.PushedDownConds)
		require.Empty(t, ds.PossibleAccessPaths[0].AccessConds)
	})

	t.Run("keeps lossy ngram LIKE as a residual prefilter", func(t *testing.T) {
		ds, _, _, bodyInfo := newMultiColumnFTSDataSource("", "NGRAM")
		anchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("foo"), ds.Schema().Columns[0],
		)
		residual := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.Like, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[1], expression.NewStrConst("abcd"), expression.NewInt64Const('\\'),
		)
		ds.AllConds = []expression.Expression{anchor, residual}
		ds.PushedDownConds = []expression.Expression{anchor, residual}

		selected, err := selectFTSIndex(ds, collectFTSConditionIndexes(ds.PushedDownConds))
		require.NoError(t, err)
		require.NotNil(t, selected)
		require.Equal(t, 1, selected.removableCondIndexes.Len())
		require.Equal(t, 2, selected.coveredCondIndexes.Len())
		require.Equal(t, 1, selected.additionalCoveredConditionCount())

		require.NoError(t, ds.resolveFTSWherePredicate())
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.AllConds)
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.PushedDownConds)

		root := ds.FtsPushDown.MatchExpr[0]
		require.Equal(t, tipb.ScalarFuncSig_LogicalAnd, root.Sig)
		require.Len(t, root.Children, 2)
		prefilter, err := expression.DecodeFTSQueryClause(*root.Children[1])
		require.NoError(t, err)
		require.Equal(t, "abcd", prefilter.Query)
		require.Equal(t, bodyInfo.ID, prefilter.Fields[0].ColID)
		require.Equal(t, string(ast.FullTextParserTypeNgramV1), prefilter.Fields[0].IndexField)

		rebuiltAnchor, ok := CheckExprCoveredByColumnarFTSAndRebuild(
			ds.SCtx().GetExprCtx(), anchor, ds.TableInfo, ds.TableInfo.Indices[0],
		)
		require.True(t, ok)
		anchorPB, err := expression.FTSQueryExpressionToPB(
			ds.SCtx().GetExprCtx().GetEvalCtx(), ds.SCtx().GetClient(), rebuiltAnchor,
		)
		require.NoError(t, err)
		require.True(t, expression.FTSQueryPBMatchesScoringExpr(&root, anchorPB))
		prefilter, err = expression.DecodeFTSQueryClause(*root.Children[1])
		require.NoError(t, err)
		require.Equal(t, expression.FtsScoreModeFilter, prefilter.ScoreMode)
	})

	t.Run("pushes every char ngram candidate as an AND subtree", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("", "NGRAM")
		ds.TableInfo.Indices[0].FullTextInfo.IndexFields[2].IndexFieldOption.Ngram = &ast.FullTextNgramOption{
			MinGram:     ast.DefaultFullTextNgramMinGram,
			MaxGram:     ast.DefaultFullTextNgramMaxGram,
			Granularity: ast.FullTextNgramGranularityChar,
			LowerCase:   ast.DefaultFullTextNgramLowerCase,
		}
		anchor := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("foo"), ds.Schema().Columns[0],
		)
		residual := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.Like, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[1], expression.NewStrConst("abcd%wxyz"), expression.NewInt64Const('\\'),
		)
		ds.AllConds = []expression.Expression{anchor, residual}
		ds.PushedDownConds = []expression.Expression{anchor, residual}

		require.NoError(t, ds.resolveFTSWherePredicate())
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.AllConds)
		requireExpressionSliceIdentity(t, []expression.Expression{residual}, ds.PushedDownConds)
		root := ds.FtsPushDown.MatchExpr[0]
		require.Equal(t, tipb.ScalarFuncSig_LogicalAnd, root.Sig)
		require.Len(t, root.Children, 2)
		prefilter := root.Children[1]
		require.Equal(t, tipb.ScalarFuncSig_LogicalAnd, prefilter.Sig)
		require.Len(t, prefilter.Children, 2)
		for i, query := range []string{"abcd", "wxyz"} {
			clause, err := expression.DecodeFTSQueryClause(*prefilter.Children[i])
			require.NoError(t, err)
			require.Equal(t, query, clause.Query)
		}

		// CNF can preserve nested AND subtrees, so matching descends through
		// AND nodes and marks the non-scoring siblings at every level.
		nestedCandidate := *prefilter.Children[0]
		require.True(t, expression.FTSQueryPBMatchesScoringExpr(&root, nestedCandidate))
		anchorClause, err := expression.DecodeFTSQueryClause(*root.Children[0])
		require.NoError(t, err)
		require.Equal(t, expression.FtsScoreModeFilter, anchorClause.ScoreMode)
		require.Empty(t, prefilter.Val)
		nestedClause, err := expression.DecodeFTSQueryClause(*prefilter.Children[0])
		require.NoError(t, err)
		require.Equal(t, expression.FtsScoreModeInherit, nestedClause.ScoreMode)
		nestedSiblingClause, err := expression.DecodeFTSQueryClause(*prefilter.Children[1])
		require.NoError(t, err)
		require.Equal(t, expression.FtsScoreModeFilter, nestedSiblingClause.ScoreMode)
	})

	t.Run("rejects a lossy rewrite inside an explicit FTS tree", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("", "NGRAM")
		match := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble),
			expression.NewStrConst("foo"), ds.Schema().Columns[0],
		)
		lossy := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.Like, types.NewFieldType(mysql.TypeTiny),
			ds.Schema().Columns[1], expression.NewStrConst("abcd"), expression.NewInt64Const('\\'),
		)
		predicate := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(), ast.LogicOr, types.NewFieldType(mysql.TypeTiny), match, lossy,
		)
		ds.AllConds = []expression.Expression{predicate}
		ds.PushedDownConds = []expression.Expression{predicate}

		require.ErrorContains(t, ds.resolveFTSWherePredicate(), "matching current TiFlash full-text index path")
		requireExpressionSliceIdentity(t, []expression.Expression{predicate}, ds.AllConds)
		requireExpressionSliceIdentity(t, []expression.Expression{predicate}, ds.PushedDownConds)
		require.Nil(t, ds.FtsPushDown)
	})

	t.Run("match word rejects an implicitly selected exact analyzer", func(t *testing.T) {
		ds, _, _, _ := newMultiColumnFTSDataSource("", "")

		require.ErrorContains(t, ds.resolveFTSWherePredicate(), "does not support fields using the EXACT parser")
		require.NotEmpty(t, ds.AllConds)
		require.NotEmpty(t, ds.PushedDownConds)
		require.Nil(t, ds.FtsPushDown)
	})

	t.Run("planner truth wrapper eligibility checks only the direct child", func(t *testing.T) {
		ds, canonical, _ := newDataSource()
		canonical.ForceKeepOrder = false
		predicate := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.IsTruthWithNull,
			types.NewFieldType(mysql.TypeTiny),
			ds.AllConds[0],
		)
		wrapper, ok := predicate.(*expression.ScalarFunction)
		require.True(t, ok)
		wrapper.RetType = types.NewFieldType(mysql.TypeGeometry)
		ds.AllConds = []expression.Expression{predicate}
		ds.PushedDownConds = []expression.Expression{predicate}
		ds.SCtx().GetSessionVars().StmtCtx.InExplainStmt = true
		warningCount := len(ds.SCtx().GetSessionVars().StmtCtx.GetWarnings())

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.NotNil(t, ds.FtsPushDown)
		require.Len(t, ds.SCtx().GetSessionVars().StmtCtx.GetWarnings(), warningCount)
	})

	t.Run("metadata order wins over current path order", func(t *testing.T) {
		ds, pathA, columnInfo := newDataSource()
		indexA := ds.TableInfo.Indices[0]
		indexB := newIndex(columnInfo, 22, "ft_title_b", ast.FullTextParserTypeMultilingualV1)
		pathB := newPath(indexB)
		ds.TableInfo.Indices = []*model.IndexInfo{indexA, indexB}
		ds.PossibleAccessPaths = []*util.AccessPath{pathB, pathA}

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Equal(t, indexA.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, string(ast.FullTextParserTypeStandardV1), decodeQueryClause(t, ds.FtsPushDown).Fields[0].IndexField)
		require.Equal(t, []*util.AccessPath{pathA}, ds.PossibleAccessPaths)
	})

	t.Run("index covering more pushed-down conditions wins", func(t *testing.T) {
		ds, pathA, titleInfo := newDataSource()
		indexA := ds.TableInfo.Indices[0]
		bodyInfo := &model.ColumnInfo{
			ID:        12,
			Name:      ast.NewCIStr("body"),
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
		}
		bodyColumn := &expression.Column{
			UniqueID: 42,
			ID:       bodyInfo.ID,
			RetType:  bodyInfo.FieldType.Clone(),
			OrigName: "test.t.body",
		}
		indexB := &model.IndexInfo{
			ID:    22,
			Name:  ast.NewCIStr("ft_title_body"),
			State: model.StatePublic,
			Columns: []*model.IndexColumn{
				{Name: titleInfo.Name, Offset: 0},
				{Name: bodyInfo.Name, Offset: 1},
			},
			FullTextInfo: &ast.FullTextIndexInfo{IndexFields: []ast.FullTextIndexFieldInfo{
				{
					ColumnID: titleInfo.ID,
					IndexFieldOption: ast.FullTextIndexFieldOption{
						AnalyzerType: ast.FullTextParserTypeMultilingualV1,
					},
				},
				{
					ColumnID: bodyInfo.ID,
					IndexFieldOption: ast.FullTextIndexFieldOption{
						AnalyzerType: ast.FullTextParserTypeExactV1,
					},
				},
			}},
		}
		pathB := newPath(indexB)
		ds.TableInfo.Columns = append(ds.TableInfo.Columns, bodyInfo)
		ds.TableInfo.Indices = []*model.IndexInfo{indexA, indexB}
		ds.PossibleAccessPaths = []*util.AccessPath{pathA, pathB}
		ds.Columns = append(ds.Columns, bodyInfo)
		ds.Schema().Append(bodyColumn)
		exactCondition := expression.NewFunctionInternal(
			ds.SCtx().GetExprCtx(),
			ast.EQ,
			types.NewFieldType(mysql.TypeTiny),
			bodyColumn,
			expression.NewStrConst("needle"),
		)
		ds.AllConds = append(ds.AllConds, exactCondition)
		ds.PushedDownConds = append(ds.PushedDownConds, exactCondition)

		selected, err := selectFTSIndex(ds, collectFTSConditionIndexes(ds.PushedDownConds))
		require.NoError(t, err)
		require.NotNil(t, selected)
		require.Same(t, indexB, selected.index)
		require.Equal(t, 2, selected.removableCondIndexes.Len())
		require.Equal(t, 2, selected.coveredCondIndexes.Len())
		require.Equal(t, 1, selected.additionalCoveredConditionCount())
		requiredInfo := expression.InterpretFullTextSearchExpr(selected.requiredRebuilt[0])
		require.NotNil(t, requiredInfo)
		require.Equal(t, string(ast.FullTextParserTypeMultilingualV1), requiredInfo.Clause.Fields[0].IndexField)

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Equal(t, indexB.ID, ds.FtsPushDown.IndexId)
		require.Equal(t, []*util.AccessPath{pathB}, ds.PossibleAccessPaths)
		require.Empty(t, ds.AllConds)
		require.Empty(t, ds.PushedDownConds)
	})

	t.Run("selector returns metadata winner and complete matched set", func(t *testing.T) {
		ds, pathA, columnInfo := newDataSource()
		ds.PushedDownConds = append(ds.PushedDownConds, newFTSExpr(ds, "world"))
		indexA := ds.TableInfo.Indices[0]
		indexB := newIndex(columnInfo, 22, "ft_title_b", ast.FullTextParserTypeMultilingualV1)
		pathB := newPath(indexB)
		ds.TableInfo.Indices = []*model.IndexInfo{indexA, indexB}
		ds.PossibleAccessPaths = []*util.AccessPath{pathB, pathA}
		paths := append([]*util.AccessPath(nil), ds.PossibleAccessPaths...)

		selected, err := selectFTSIndex(ds, collectFTSConditionIndexes(ds.PushedDownConds))
		require.NoError(t, err)
		require.NotNil(t, selected)
		require.Same(t, indexA, selected.index)
		require.Same(t, pathA, selected.path)
		require.Equal(t, 2, selected.matchedCondIndexes.Len())
		require.True(t, selected.matchedCondIndexes.Has(0))
		require.True(t, selected.matchedCondIndexes.Has(1))
		require.Equal(t, 2, selected.coveredCondIndexes.Len())
		require.Equal(t, paths, ds.PossibleAccessPaths)
	})

	for _, testCase := range []struct {
		name   string
		mutate func(*DataSource)
	}{
		{
			name: "semantically equal distinct pointer is not ownership",
			mutate: func(ds *DataSource) {
				ds.AllConds = []expression.Expression{ds.AllConds[0].Clone()}
			},
		},
		{
			name: "extra unpushed FTS expression is not ownership",
			mutate: func(ds *DataSource) {
				ds.AllConds = append(ds.AllConds, newFTSExpr(ds, "world"))
			},
		},
		{
			name: "AllConds-only duplicate is not ownership",
			mutate: func(ds *DataSource) {
				ds.AllConds = append(ds.AllConds, ds.AllConds[0])
			},
		},
		{
			name: "PushedDownConds-only duplicate is not ownership",
			mutate: func(ds *DataSource) {
				ds.PushedDownConds = append(ds.PushedDownConds, ds.PushedDownConds[0])
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ds, _, _ := newDataSource()
			testCase.mutate(ds)
			requireLocalNoOp(t, ds)
		})
	}

	t.Run("enum eligibility appends exactly one existing warning", func(t *testing.T) {
		ds, canonical, columnInfo := newDataSource()
		columnInfo.FieldType = *types.NewFieldType(mysql.TypeEnum)
		ds.Schema().Columns[0].RetType = columnInfo.FieldType.Clone()
		ds.SCtx().GetSessionVars().StmtCtx.InExplainStmt = true
		allConds := append([]expression.Expression(nil), ds.AllConds...)
		pushedDownConds := append([]expression.Expression(nil), ds.PushedDownConds...)
		pathSnapshot := *canonical
		planColumnID := ds.SCtx().GetSessionVars().PlanColumnID.Load()
		warnings := ds.SCtx().GetSessionVars().StmtCtx.GetWarnings()

		require.ErrorContains(t, ds.resolveFTSWherePredicate(), "cannot be pushed down to TiFlash")
		updatedWarnings := ds.SCtx().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, updatedWarnings, len(warnings)+1)
		require.ErrorContains(t, updatedWarnings[len(warnings)].Err, "TiFlash")
		require.ErrorContains(t, updatedWarnings[len(warnings)].Err, "enum")
		requireExpressionSliceIdentity(t, allConds, ds.AllConds)
		requireExpressionSliceIdentity(t, pushedDownConds, ds.PushedDownConds)
		require.Equal(t, pathSnapshot, *canonical)
		require.Nil(t, ds.FtsPushDown)
		require.Equal(t, planColumnID, ds.SCtx().GetSessionVars().PlanColumnID.Load())
	})

	for _, testCase := range []struct {
		name        string
		funcName    string
		nestTruth   bool
		errContains string
		consumed    bool
	}{
		{name: "explicit is true reports a path mismatch", funcName: ast.IsTruthWithoutNull, errContains: "matching current TiFlash full-text index path"},
		{name: "nested planner truth is consumed", funcName: ast.IsTruthWithNull, nestTruth: true, consumed: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ds, canonical, _ := newDataSource()
			direct := ds.AllConds[0]
			if testCase.nestTruth {
				direct = expression.NewFunctionInternal(
					ds.SCtx().GetExprCtx(), ast.IsTruthWithNull, types.NewFieldType(mysql.TypeTiny), direct,
				)
			}
			predicate := expression.NewFunctionInternal(
				ds.SCtx().GetExprCtx(), testCase.funcName, types.NewFieldType(mysql.TypeTiny), direct,
			)
			ds.AllConds = []expression.Expression{predicate}
			ds.PushedDownConds = []expression.Expression{predicate}

			err := ds.resolveFTSWherePredicate()
			if testCase.errContains != "" {
				require.ErrorContains(t, err, testCase.errContains)
			} else {
				require.NoError(t, err)
			}
			if testCase.consumed {
				require.Empty(t, ds.AllConds)
				require.Empty(t, ds.PushedDownConds)
				require.NotNil(t, ds.FtsPushDown)
			} else {
				require.True(t, expression.ContainsFullTextSearchFn(ds.AllConds[0]))
				require.Nil(t, ds.FtsPushDown)
			}
			require.Equal(t, []*util.AccessPath{canonical}, ds.PossibleAccessPaths)
		})
	}

	t.Run("distinct direct predicates consume neither", func(t *testing.T) {
		ds, canonical, columnInfo := newDataSource()
		second := newFTSExpr(ds, "world")
		ds.AllConds = append(ds.AllConds, second)
		ds.PushedDownConds = append(ds.PushedDownConds, second)

		require.NoError(t, ds.resolveFTSWherePredicate())
		require.Len(t, ds.AllConds, 2)
		require.Len(t, ds.PushedDownConds, 2)
		require.Nil(t, ds.FtsPushDown)
		require.Equal(t, []*util.AccessPath{canonical}, ds.PossibleAccessPaths)
		require.Equal(t, []*model.ColumnInfo{columnInfo}, ds.Columns)
	})

	t.Run("data sources resolve independently", func(t *testing.T) {
		legal, _, _ := newDataSource()
		invalid, invalidPath, _ := newDataSource()
		invalidPredicate := expression.NewFunctionInternal(
			invalid.SCtx().GetExprCtx(), ast.IsTruthWithoutNull, types.NewFieldType(mysql.TypeTiny), invalid.AllConds[0],
		)
		invalid.AllConds = []expression.Expression{invalidPredicate}
		invalid.PushedDownConds = []expression.Expression{invalidPredicate}

		require.NoError(t, legal.resolveFTSWherePredicate())
		require.ErrorContains(t, invalid.resolveFTSWherePredicate(), "matching current TiFlash full-text index path")
		require.NotNil(t, legal.FtsPushDown)
		require.Nil(t, invalid.FtsPushDown)
		require.True(t, expression.ContainsFullTextSearchFn(invalid.AllConds[0]))
		require.Equal(t, []*util.AccessPath{invalidPath}, invalid.PossibleAccessPaths)
	})

	t.Run("one data source hint conflict does not roll back another", func(t *testing.T) {
		legal, _, _ := newDataSource()
		conflicting, conflictingPath, _ := newDataSource()
		conflicting.IndexMergeHints = []h.HintedIndex{{}}

		require.NoError(t, legal.resolveFTSWherePredicate())
		require.ErrorContains(t, conflicting.resolveFTSWherePredicate(), "INDEX_MERGE")
		require.NotNil(t, legal.FtsPushDown)
		require.Nil(t, conflicting.FtsPushDown)
		require.Equal(t, []*util.AccessPath{conflictingPath}, conflicting.PossibleAccessPaths)
	})
}

func TestCheckExprCoveredByColumnarFTSAndRebuild(t *testing.T) {
	setupStarterFTSTest(t)
	sctx := mock.NewContext()
	location := time.FixedZone("UTC-07", -7*60*60)
	sctx.GetSessionVars().TimeZone = location
	sctx.GetSessionVars().StmtCtx.SetTimeZone(location)
	ctx := sctx.GetExprCtx()
	boolType := types.NewFieldType(mysql.TypeTiny)
	stringType := types.NewFieldType(mysql.TypeVarchar)
	intType := types.NewFieldType(mysql.TypeLong)
	floatType := types.NewFieldType(mysql.TypeFloat)
	bitType := types.NewFieldType(mysql.TypeBit)
	bitType.SetFlen(8)
	enumType := types.NewFieldType(mysql.TypeEnum)
	enumType.SetCharset(charset.CharsetUTF8MB4)
	enumType.SetCollate("utf8mb4_general_ci")
	enumType.SetElems([]string{"small", "medium", "large"})
	temporalType := func(tp byte, fsp int) *types.FieldType {
		fieldType := types.NewFieldType(tp)
		fieldType.SetDecimal(fsp)
		return fieldType
	}

	exactInfo := &model.ColumnInfo{ID: 11, Name: ast.NewCIStr("exact_col"), State: model.StatePublic, FieldType: *stringType.Clone()}
	textInfo := &model.ColumnInfo{ID: 12, Name: ast.NewCIStr("text_col"), State: model.StatePublic, FieldType: *stringType.Clone()}
	pathInfo := &model.ColumnInfo{ID: 13, Name: ast.NewCIStr("path_col"), State: model.StatePublic, FieldType: *stringType.Clone()}
	numberInfo := &model.ColumnInfo{ID: 14, Name: ast.NewCIStr("number_col"), State: model.StatePublic, FieldType: *intType.Clone()}
	uncoveredInfo := &model.ColumnInfo{ID: 15, Name: ast.NewCIStr("uncovered_col"), State: model.StatePublic, FieldType: *stringType.Clone()}
	dateInfo := &model.ColumnInfo{ID: 16, Name: ast.NewCIStr("date_col"), State: model.StatePublic, FieldType: *temporalType(mysql.TypeDate, 0)}
	datetimeInfo := &model.ColumnInfo{ID: 17, Name: ast.NewCIStr("datetime_col"), State: model.StatePublic, FieldType: *temporalType(mysql.TypeDatetime, 6)}
	timestampInfo := &model.ColumnInfo{ID: 18, Name: ast.NewCIStr("timestamp_col"), State: model.StatePublic, FieldType: *temporalType(mysql.TypeTimestamp, 6)}
	durationInfo := &model.ColumnInfo{ID: 19, Name: ast.NewCIStr("duration_col"), State: model.StatePublic, FieldType: *temporalType(mysql.TypeDuration, 6)}
	floatInfo := &model.ColumnInfo{ID: 20, Name: ast.NewCIStr("float_col"), State: model.StatePublic, FieldType: *floatType.Clone()}
	bitInfo := &model.ColumnInfo{ID: 22, Name: ast.NewCIStr("bit_col"), State: model.StatePublic, FieldType: *bitType.Clone()}
	enumInfo := &model.ColumnInfo{ID: 23, Name: ast.NewCIStr("enum_col"), State: model.StatePublic, FieldType: *enumType.Clone()}
	table := &model.TableInfo{
		ID:   21,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			exactInfo, textInfo, pathInfo, numberInfo, uncoveredInfo,
			dateInfo, datetimeInfo, timestampInfo, durationInfo, floatInfo, bitInfo,
			enumInfo,
		},
	}
	index := &model.IndexInfo{
		ID:    31,
		Name:  ast.NewCIStr("fts"),
		State: model.StatePublic,
		Columns: []*model.IndexColumn{
			{Name: exactInfo.Name, Offset: 0},
			{Name: textInfo.Name, Offset: 1},
			{Name: pathInfo.Name, Offset: 2},
			{Name: numberInfo.Name, Offset: 3},
			{Name: dateInfo.Name, Offset: 5},
			{Name: datetimeInfo.Name, Offset: 6},
			{Name: timestampInfo.Name, Offset: 7},
			{Name: durationInfo.Name, Offset: 8},
			{Name: floatInfo.Name, Offset: 9},
			{Name: bitInfo.Name, Offset: 10},
			{Name: enumInfo.Name, Offset: 11},
		},
		FullTextInfo: &ast.FullTextIndexInfo{IndexFields: []ast.FullTextIndexFieldInfo{
			{
				ColumnID: exactInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: textInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeStandardV1,
				},
			},
			{
				ColumnID: textInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeNgramV1,
					Ngram: &ast.FullTextNgramOption{
						MinGram:     3,
						MaxGram:     4,
						Granularity: ast.FullTextNgramGranularityChar,
					},
				},
			},
			{
				ColumnID: pathInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypePathHierarchyV1,
					PathHierarchy: &ast.FullTextPathHierarchyOption{
						Delimiter: ">",
					},
				},
			},
			{
				ColumnID: numberInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: dateInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: datetimeInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: timestampInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: durationInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: floatInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: bitInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
			{
				ColumnID: enumInfo.ID,
				IndexFieldOption: ast.FullTextIndexFieldOption{
					AnalyzerType: ast.FullTextParserTypeExactV1,
				},
			},
		}},
	}
	table.Indices = []*model.IndexInfo{index}

	column := func(info *model.ColumnInfo, uniqueID int64) *expression.Column {
		return &expression.Column{
			UniqueID: uniqueID,
			ID:       info.ID,
			RetType:  info.FieldType.Clone(),
			OrigName: "test.t." + info.Name.O,
		}
	}
	exactColumn := column(exactInfo, 41)
	textColumn := column(textInfo, 42)
	pathColumn := column(pathInfo, 43)
	numberColumn := column(numberInfo, 44)
	uncoveredColumn := column(uncoveredInfo, 45)
	dateColumn := column(dateInfo, 46)
	datetimeColumn := column(datetimeInfo, 47)
	timestampColumn := column(timestampInfo, 48)
	durationColumn := column(durationInfo, 49)
	floatColumn := column(floatInfo, 50)
	bitColumn := column(bitInfo, 51)
	enumColumn := column(enumInfo, 52)
	newScalar := func(name string, args ...expression.Expression) *expression.ScalarFunction {
		t.Helper()
		expr := expression.NewFunctionInternal(ctx, name, boolType.Clone(), args...)
		require.NotNil(t, expr)
		result, ok := expr.(*expression.ScalarFunction)
		require.True(t, ok)
		return result
	}
	newFTSMatchWord := func(args ...expression.Expression) *expression.ScalarFunction {
		t.Helper()
		expr := expression.NewFunctionInternal(ctx, ast.FTSMatchWord, types.NewFieldType(mysql.TypeDouble), args...)
		require.NotNil(t, expr)
		result, ok := expr.(*expression.ScalarFunction)
		require.True(t, ok)
		return result
	}
	requireBoundParser := func(t *testing.T, expr expression.Expression, expected ast.FullTextParserType) {
		t.Helper()
		info := expression.InterpretFullTextSearchExpr(expr)
		require.NotNil(t, info)
		require.Len(t, info.Clause.Fields, 1)
		require.Equal(t, string(expected), info.Clause.Fields[0].IndexField)
	}

	t.Run("exact equality supports both operand orders and numeric values", func(t *testing.T) {
		for _, testCase := range []struct {
			expr  expression.Expression
			query any
		}{
			{newScalar(ast.EQ, exactColumn, expression.NewStrConst("hello")), "hello"},
			{newScalar(ast.EQ, expression.NewStrConst("hello"), exactColumn), "hello"},
			{newScalar(ast.EQ, numberColumn, expression.NewInt64Const(42)), int64(42)},
		} {
			rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, testCase.expr, table, index)
			require.True(t, ok)
			require.NotSame(t, testCase.expr, rebuilt)
			scalar := rebuilt.(*expression.ScalarFunction)
			require.Equal(t, ast.FTSMatchWord, scalar.FuncName.L)
			info := expression.InterpretFullTextSearchExpr(rebuilt)
			require.NotNil(t, info)
			require.Equal(t, testCase.query, info.Clause.Query)
			require.Equal(t, string(ast.FullTextParserTypeExactV1), info.Clause.Fields[0].IndexField)
		}
	})

	t.Run("native exact pushdown preserves TiDB equality semantics", func(t *testing.T) {
		realConstant := func(value float64) *expression.Constant {
			return &expression.Constant{
				Value:   types.NewFloat64Datum(value),
				RetType: types.NewFieldType(mysql.TypeDouble),
			}
		}
		dateValue, err := types.ParseTime(
			types.StrictContext.WithLocation(location), "2026-08-13", mysql.TypeDate, 0,
		)
		require.NoError(t, err)
		datePacked, err := dateValue.ToPackedUint()
		require.NoError(t, err)
		bitValue := func(value uint64) types.Datum {
			return types.NewMysqlBitDatum(types.NewBinaryLiteralFromUint(value, 1))
		}
		assertEqualityAndPushdown := func(
			t *testing.T,
			predicate expression.Expression,
			storedValues []types.Datum,
			ordinaryResults []bool,
			pushable bool,
			expectedQuery any,
		) {
			t.Helper()
			require.Len(t, ordinaryResults, len(storedValues))
			for i, storedValue := range storedValues {
				matched, isNull, err := expression.EvalBool(
					ctx.GetEvalCtx(),
					expression.CNFExprs{predicate},
					chunk.MutRowFromDatums([]types.Datum{storedValue}).ToRow(),
				)
				require.NoError(t, err)
				require.False(t, isNull)
				require.Equal(t, ordinaryResults[i], matched)
			}

			rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, predicate, table, index)
			require.Equal(t, pushable, ok)
			if !pushable {
				require.Nil(t, rebuilt)
				return
			}
			info := expression.InterpretFullTextSearchExpr(rebuilt)
			require.NotNil(t, info)
			require.Equal(t, expectedQuery, info.Clause.Query)
		}

		for _, testCase := range []struct {
			name            string
			predicate       expression.Expression
			storedValues    []types.Datum
			ordinaryResults []bool
			pushable        bool
			expectedQuery   any
		}{
			{
				name:            "lossy float",
				predicate:       newScalar(ast.EQ, floatColumn, realConstant(0.1)),
				storedValues:    []types.Datum{types.NewFloat32Datum(float32(0.1))},
				ordinaryResults: []bool{false},
			},
			{
				name:            "lossy float reversed",
				predicate:       newScalar(ast.EQ, realConstant(0.1), floatColumn),
				storedValues:    []types.Datum{types.NewFloat32Datum(float32(0.1))},
				ordinaryResults: []bool{false},
			},
			{
				name:            "exact float",
				predicate:       newScalar(ast.EQ, floatColumn, realConstant(0.5)),
				storedValues:    []types.Datum{types.NewFloat32Datum(0.5)},
				ordinaryResults: []bool{true},
				pushable:        true,
				expectedQuery:   float64(0.5),
			},
			{
				name:            "exact date",
				predicate:       newScalar(ast.EQ, dateColumn, expression.NewStrConst("2026-08-13")),
				storedValues:    []types.Datum{types.NewDatum(dateValue)},
				ordinaryResults: []bool{true},
				pushable:        true,
				expectedQuery:   datePacked,
			},
			{
				name:            "date with nonzero time",
				predicate:       newScalar(ast.EQ, dateColumn, expression.NewStrConst("2026-08-13 12:00:00")),
				storedValues:    []types.Datum{types.NewDatum(dateValue)},
				ordinaryResults: []bool{false},
			},
			{
				name:            "date with nonzero time reversed",
				predicate:       newScalar(ast.EQ, expression.NewStrConst("2026-08-13 12:00:00"), dateColumn),
				storedValues:    []types.Datum{types.NewDatum(dateValue)},
				ordinaryResults: []bool{false},
			},
			{
				name:            "bit numeric",
				predicate:       newScalar(ast.EQ, bitColumn, expression.NewInt64Const(1)),
				storedValues:    []types.Datum{bitValue(1), bitValue(49)},
				ordinaryResults: []bool{true, false},
				pushable:        true,
				expectedQuery:   uint64(1),
			},
			{
				name:            "bit string uses comparison coercion",
				predicate:       newScalar(ast.EQ, bitColumn, expression.NewStrConst("1")),
				storedValues:    []types.Datum{bitValue(1), bitValue(49)},
				ordinaryResults: []bool{true, false},
				pushable:        true,
				expectedQuery:   uint64(1),
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				assertEqualityAndPushdown(
					t, testCase.predicate, testCase.storedValues, testCase.ordinaryResults,
					testCase.pushable, testCase.expectedQuery,
				)
			})
		}

		t.Run("unsafe IN members keep the whole predicate residual", func(t *testing.T) {
			for _, testCase := range []struct {
				predicate   expression.Expression
				storedValue types.Datum
				ordinary    bool
			}{
				{
					predicate:   newScalar(ast.In, floatColumn, realConstant(0.1)),
					storedValue: types.NewFloat32Datum(float32(0.1)),
				},
				{
					predicate:   newScalar(ast.In, floatColumn, realConstant(0.5), realConstant(0.1)),
					storedValue: types.NewFloat32Datum(0.5),
					ordinary:    true,
				},
				{
					predicate:   newScalar(ast.In, dateColumn, expression.NewStrConst("2026-08-13 12:00:00")),
					storedValue: types.NewDatum(dateValue),
				},
			} {
				assertEqualityAndPushdown(
					t,
					testCase.predicate,
					[]types.Datum{testCase.storedValue},
					[]bool{testCase.ordinary},
					false,
					nil,
				)
			}
		})

		t.Run("BIT string IN uses numeric comparison value", func(t *testing.T) {
			predicate := newScalar(ast.In, bitColumn, expression.NewStrConst("1"))
			assertEqualityAndPushdown(
				t,
				predicate,
				[]types.Datum{bitValue(1), bitValue(49)},
				[]bool{true, false},
				true,
				uint64(1),
			)
		})
	})

	t.Run("enum exact predicates preserve label and ordinal semantics", func(t *testing.T) {
		enumDatum := func(name string, value uint64, fieldType *types.FieldType) types.Datum {
			return types.NewCollateMysqlEnumDatum(types.Enum{Name: name, Value: value}, fieldType.GetCollate())
		}
		realConstant := func(value float64) *expression.Constant {
			return &expression.Constant{Value: types.NewFloat64Datum(value), RetType: types.NewFieldType(mysql.TypeDouble)}
		}
		collectQueries := func(root expression.Expression) []uint64 {
			queries := make([]uint64, 0, 2)
			var visit func(expression.Expression)
			visit = func(expr expression.Expression) {
				if info := expression.InterpretFullTextSearchExpr(expr); info != nil {
					queries = append(queries, info.Clause.Query.(uint64))
					require.IsType(t, &expression.Column{}, expr.(*expression.ScalarFunction).GetArgs()[1])
					return
				}
				for _, arg := range expr.(*expression.ScalarFunction).GetArgs() {
					visit(arg)
				}
			}
			visit(root)
			return queries
		}
		check := func(t *testing.T, predicate expression.Expression, stored types.Datum, matches, pushable bool, queries ...uint64) {
			t.Helper()
			matched, isNull, err := expression.EvalBool(
				ctx.GetEvalCtx(), expression.CNFExprs{predicate}, chunk.MutRowFromDatums([]types.Datum{stored}).ToRow(),
			)
			require.NoError(t, err)
			require.False(t, isNull)
			require.Equal(t, matches, matched)
			rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, predicate, table, index)
			require.Equal(t, pushable, ok)
			if !pushable {
				require.Nil(t, rebuilt)
				return
			}
			require.Equal(t, queries, collectQueries(rebuilt))
		}

		zero := enumDatum("", 0, enumType)
		small := enumDatum("small", 1, enumType)
		medium := enumDatum("medium", 2, enumType)
		caseInsensitiveLabel := newScalar(ast.EQ, enumColumn, expression.NewStrConst("MEDIUM"))
		_, comparisonCollation := caseInsensitiveLabel.CharsetAndCollation()
		require.Equal(t, enumType.GetCollate(), comparisonCollation)

		numericLabelColumn := enumColumn.Clone().(*expression.Column)
		numericLabelColumn.RetType = numericLabelColumn.RetType.Clone()
		numericLabelColumn.RetType.SetElems([]string{"2", "other"})
		numericLabel := enumDatum("2", 1, numericLabelColumn.RetType)
		ordinalTwo := enumDatum("other", 2, numericLabelColumn.RetType)
		emptyLabelColumn := enumColumn.Clone().(*expression.Column)
		emptyLabelColumn.RetType = emptyLabelColumn.RetType.Clone()
		emptyLabelColumn.RetType.SetElems([]string{"", "other"})

		for _, testCase := range []struct {
			name      string
			predicate expression.Expression
			stored    types.Datum
			matches   bool
			pushable  bool
			queries   []uint64
		}{
			{"label", newScalar(ast.EQ, enumColumn, expression.NewStrConst("medium")), medium, true, true, []uint64{2}},
			{"label reversed", newScalar(ast.EQ, expression.NewStrConst("medium"), enumColumn), medium, true, true, []uint64{2}},
			{"label collation", caseInsensitiveLabel, medium, true, true, []uint64{2}},
			{"ordinal", newScalar(ast.EQ, enumColumn, expression.NewInt64Const(2)), medium, true, true, []uint64{2}},
			{"ordinal reversed", newScalar(ast.EQ, expression.NewInt64Const(2), enumColumn), medium, true, true, []uint64{2}},
			{"empty label zero", newScalar(ast.EQ, enumColumn, expression.NewStrConst("")), zero, true, true, []uint64{0}},
			{"numeric zero", newScalar(ast.EQ, enumColumn, expression.NewInt64Const(0)), zero, true, true, []uint64{0}},
			{"numeric-looking label", newScalar(ast.EQ, numericLabelColumn, expression.NewStrConst("2")), numericLabel, true, true, []uint64{1}},
			{"numeric-looking ordinal", newScalar(ast.EQ, numericLabelColumn, expression.NewInt64Const(2)), ordinalTwo, true, true, []uint64{2}},
			{"unknown label", newScalar(ast.EQ, enumColumn, expression.NewStrConst("unknown")), medium, false, false, nil},
			{"out-of-range ordinal", newScalar(ast.EQ, enumColumn, expression.NewInt64Const(4)), medium, false, false, nil},
			{"fractional ordinal", newScalar(ast.EQ, enumColumn, realConstant(2.5)), medium, false, false, nil},
			{"ambiguous empty label", newScalar(ast.EQ, emptyLabelColumn, expression.NewStrConst("")), zero, true, false, nil},
			{"IN labels", newScalar(ast.In, enumColumn, expression.NewStrConst("small"), expression.NewStrConst("large")), small, true, true, []uint64{1, 3}},
			{"unsafe IN member", newScalar(ast.In, enumColumn, expression.NewStrConst("small"), expression.NewStrConst("unknown")), small, true, false, nil},
			{"numeric IN uses label", newScalar(ast.In, numericLabelColumn, expression.NewInt64Const(2)), numericLabel, true, true, []uint64{1}},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				check(t, testCase.predicate, testCase.stored, testCase.matches, testCase.pushable, testCase.queries...)
			})
		}
	})
	t.Run("temporal exact equality remains directly interpretable", func(t *testing.T) {
		date, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13", mysql.TypeDate, 0)
		require.NoError(t, err)
		datePacked, err := date.ToPackedUint()
		require.NoError(t, err)

		datetime, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13 12:34:56.123456", mysql.TypeDatetime, 6)
		require.NoError(t, err)
		datetimePacked, err := datetime.ToPackedUint()
		require.NoError(t, err)

		timestamp, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13 12:34:56.123456", mysql.TypeTimestamp, 6)
		require.NoError(t, err)
		require.NoError(t, timestamp.ConvertTimeZone(location, time.UTC))
		timestampPacked, err := timestamp.ToPackedUint()
		require.NoError(t, err)

		duration, _, err := types.ParseDuration(types.StrictContext.WithLocation(location), "12:34:56.123456", 6)
		require.NoError(t, err)

		for _, testCase := range []struct {
			name     string
			column   *expression.Column
			literal  string
			expected any
		}{
			{name: "date", column: dateColumn, literal: "2026-08-13", expected: datePacked},
			{name: "datetime", column: datetimeColumn, literal: "2026-08-13 12:34:56.123456", expected: datetimePacked},
			{name: "timestamp uses UTC storage encoding", column: timestampColumn, literal: "2026-08-13 12:34:56.123456", expected: timestampPacked},
			{name: "duration", column: durationColumn, literal: "12:34:56.123456", expected: int64(duration.Duration)},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				input := newScalar(ast.EQ, testCase.column, expression.NewStrConst(testCase.literal))
				rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
				require.True(t, ok)

				scalar, ok := rebuilt.(*expression.ScalarFunction)
				require.True(t, ok)
				require.Equal(t, ast.FTSMatchWord, scalar.FuncName.L)
				require.Len(t, scalar.GetArgs(), 2)
				matchColumn, ok := scalar.GetArgs()[1].(*expression.Column)
				require.True(t, ok, "FTS field argument must remain a direct column")
				require.Equal(t, testCase.column.ID, matchColumn.ID)

				info := expression.InterpretFullTextSearchExpr(rebuilt)
				require.NotNil(t, info)
				require.Equal(t, testCase.expected, info.Clause.Query)
				require.Equal(t, string(ast.FullTextParserTypeExactV1), info.Clause.Fields[0].IndexField)
			})
		}
	})

	t.Run("in becomes a balanced disjunction of exact equalities", func(t *testing.T) {
		input := newScalar(
			ast.In,
			numberColumn,
			expression.NewInt64Const(1),
			expression.NewInt64Const(2),
			expression.NewInt64Const(3),
			expression.NewInt64Const(4),
		)
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		root, ok := rebuilt.(*expression.ScalarFunction)
		require.True(t, ok)
		require.Equal(t, ast.LogicOr, root.FuncName.L)
		require.Len(t, root.GetArgs(), 2)
		for _, child := range root.GetArgs() {
			childOR, ok := child.(*expression.ScalarFunction)
			require.True(t, ok)
			require.Equal(t, ast.LogicOr, childOR.FuncName.L)
		}
		matches := make([]expression.Expression, 0, 4)
		var collectMatches func(expression.Expression)
		collectMatches = func(expr expression.Expression) {
			if expression.InterpretFullTextSearchExpr(expr) != nil {
				matches = append(matches, expr)
				return
			}
			if scalar, ok := expr.(*expression.ScalarFunction); ok {
				for _, arg := range scalar.GetArgs() {
					collectMatches(arg)
				}
			}
		}
		collectMatches(root)
		require.Len(t, matches, 4)
		for _, match := range matches {
			requireBoundParser(t, match, ast.FullTextParserTypeExactV1)
		}
	})

	t.Run("like binds path hierarchy with its configured delimiter", func(t *testing.T) {
		input := newScalar(ast.Like, pathColumn, expression.NewStrConst("root>child>%"), expression.NewInt64Const('\\'))
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		require.NotSame(t, input, rebuilt)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypePathHierarchyV1)
		require.Equal(t, "root>child>", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)

		invalid := newScalar(ast.Like, pathColumn, expression.NewStrConst("root>child%"), expression.NewInt64Const('\\'))
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, invalid, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("parameterized path like uses the current execution value", func(t *testing.T) {
		paramSctx := mock.NewContext()
		paramSctx.GetSessionVars().StmtCtx.EnablePlanCache()
		paramCtx := paramSctx.GetExprCtx()
		newParameterizedLike := func(value types.Datum) (*expression.Constant, *expression.ScalarFunction) {
			paramSctx.GetSessionVars().PlanCacheParams.Reset()
			paramSctx.GetSessionVars().PlanCacheParams.Append(value)
			marker := &driver.ParamMarkerExpr{Order: 0, InExecute: true}
			marker.Datum = value
			pattern, err := expression.ParamMarkerExpression(paramCtx, marker, false)
			require.NoError(t, err)
			require.NotNil(t, pattern.ParamMarker)
			expr := expression.NewFunctionInternal(
				paramCtx, ast.Like, boolType.Clone(), pathColumn, pattern, expression.NewInt64Const('\\'),
			)
			input, ok := expr.(*expression.ScalarFunction)
			require.True(t, ok)
			boundPattern, ok := input.GetArgs()[1].(*expression.Constant)
			require.True(t, ok, "prepared LIKE pattern must remain a constant")
			require.NotNil(t, boundPattern.ParamMarker)
			return boundPattern, input
		}

		pattern, input := newParameterizedLike(types.NewStringDatum("root>child>%"))

		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(paramCtx, input, table, index)
		require.True(t, ok)
		require.NotNil(t, pattern.ParamMarker, "the source expression must remain parameterized")
		requireBoundParser(t, rebuilt, ast.FullTextParserTypePathHierarchyV1)
		require.Equal(t, "root>child>", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)

		_, invalid := newParameterizedLike(types.NewStringDatum("root>child%"))
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(paramCtx, invalid, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)

		_, nullInput := newParameterizedLike(types.NewDatum(nil))
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(paramCtx, nullInput, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("ngram LIKE is available only as a lossy prefilter", func(t *testing.T) {
		for _, testCase := range []struct {
			pattern string
			query   string
		}{
			{pattern: "%abc%", query: "abc"},
			{pattern: `%a\%bc%`, query: "a%bc"},
			{pattern: "%中文词%", query: "中文词"},
			{pattern: "abcd", query: "abcd"},
			{pattern: "abc%", query: "abc"},
			{pattern: "%abc", query: "abc"},
		} {
			pattern := testCase.pattern
			input := newScalar(ast.Like, textColumn, expression.NewStrConst(pattern), expression.NewInt64Const('\\'))
			rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
			require.False(t, ok, pattern)
			require.Nil(t, rebuilt)

			rebuilt, ok = buildColumnarFTSPrefilter(ctx, input, table, index)
			require.True(t, ok, pattern)
			requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
			require.Equal(t, testCase.query, expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)
		}
		for _, pattern := range []string{"%ab%", "%ab_%", "%%%", `%abc\`} {
			input := newScalar(ast.Like, textColumn, expression.NewStrConst(pattern), expression.NewInt64Const('\\'))
			rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
			require.False(t, ok, pattern)
			require.Nil(t, rebuilt)
		}
	})

	t.Run("char ngram prefilter keeps every mandatory literal", func(t *testing.T) {
		input := newScalar(
			ast.Like, textColumn, expression.NewStrConst("%abcd%_wxyz%abcd%"), expression.NewInt64Const('\\'),
		)
		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		root := rebuilt.(*expression.ScalarFunction)
		require.Equal(t, ast.LogicAnd, root.FuncName.L)

		queries := make([]any, 0, 2)
		var collectQueries func(expression.Expression)
		collectQueries = func(expr expression.Expression) {
			expr = unwrapFTSPredicate(expr)
			if info := expression.InterpretFullTextSearchExpr(expr); info != nil {
				requireBoundParser(t, expr, ast.FullTextParserTypeNgramV1)
				queries = append(queries, info.Clause.Query)
				return
			}
			scalar := expr.(*expression.ScalarFunction)
			require.Equal(t, ast.LogicAnd, scalar.FuncName.L)
			for _, child := range scalar.GetArgs() {
				collectQueries(child)
			}
		}
		collectQueries(root)
		require.Equal(t, []any{"abcd", "wxyz"}, queries)

	})

	t.Run("word ngram prefilter joins every qualifying word", func(t *testing.T) {
		ngram := index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram
		ngram.Granularity = ast.FullTextNgramGranularityWord
		t.Cleanup(func() {
			ngram.Granularity = ast.FullTextNgramGranularityChar
		})
		input := newScalar(
			ast.Like, textColumn, expression.NewStrConst("%alpha-beta%_xy%gamma%alpha%"), expression.NewInt64Const('\\'),
		)

		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
		require.Equal(t, "alpha beta gamma", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)
	})

	t.Run("ngram ILIKE requires lower case indexing", func(t *testing.T) {
		input := newScalar(ast.Ilike, textColumn, expression.NewStrConst("AbCd%"), expression.NewInt64Const('\\'))
		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)

		index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = true
		t.Cleanup(func() {
			index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = false
		})
		rebuilt, ok = buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
		require.Equal(t, "abcd", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)
	})

	t.Run("case insensitive LIKE requires lower case indexing", func(t *testing.T) {
		input := newScalar(ast.Like, textColumn, expression.NewStrConst("abcd"), expression.NewInt64Const('\\'))
		input.SetCharsetAndCollation("utf8mb4", "utf8mb4_0900_ai_ci")

		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)

		index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = true
		t.Cleanup(func() {
			index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = false
		})
		rebuilt, ok = buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
	})

	t.Run("case sensitive LIKE permits lower case indexing", func(t *testing.T) {
		index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = true
		t.Cleanup(func() {
			index.FullTextInfo.IndexFields[2].IndexFieldOption.Ngram.LowerCase = false
		})
		input := newScalar(ast.Like, textColumn, expression.NewStrConst("AbCd"), expression.NewInt64Const('\\'))
		input.SetCharsetAndCollation("utf8mb4", "utf8mb4_bin")

		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
		require.Equal(t, "AbCd", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)
	})

	t.Run("path prefix with a partial component becomes a parent prefilter", func(t *testing.T) {
		input := newScalar(ast.Like, pathColumn, expression.NewStrConst("root>child%"), expression.NewInt64Const('\\'))
		rebuilt, ok := buildColumnarFTSPrefilter(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypePathHierarchyV1)
		require.Equal(t, "root>", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)

		input.SetCharsetAndCollation("utf8mb4", "utf8mb4_0900_ai_ci")
		rebuilt, ok = buildColumnarFTSPrefilter(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("logical trees are rebuilt only when every leaf is covered", func(t *testing.T) {
		exact := newScalar(ast.EQ, exactColumn, expression.NewStrConst("hello"))
		numeric := newScalar(ast.EQ, numberColumn, expression.NewInt64Const(42))
		input := newScalar(ast.LogicAnd, exact, newScalar(ast.UnaryNot, numeric))
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		require.NotSame(t, input, rebuilt)

		unsupported := newScalar(ast.GT, exactColumn, expression.NewStrConst("hello"))
		input = newScalar(ast.LogicAnd, exact, unsupported)
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("lossy Boolean prefilters support partial AND and complete OR but not NOT", func(t *testing.T) {
		ngram := newScalar(ast.Like, textColumn, expression.NewStrConst("abcd"), expression.NewInt64Const('\\'))
		path := newScalar(ast.Like, pathColumn, expression.NewStrConst("root>child%"), expression.NewInt64Const('\\'))
		unsupported := newScalar(ast.GT, uncoveredColumn, expression.NewStrConst("hello"))

		rebuilt, ok := buildColumnarFTSPrefilter(ctx, newScalar(ast.LogicAnd, ngram, unsupported), table, index)
		require.True(t, ok)
		require.Equal(t, "abcd", expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)

		rebuilt, ok = buildColumnarFTSPrefilter(ctx, newScalar(ast.LogicOr, ngram, path), table, index)
		require.True(t, ok)
		root := rebuilt.(*expression.ScalarFunction)
		require.Equal(t, ast.LogicOr, root.FuncName.L)
		require.Len(t, root.GetArgs(), 2)

		rebuilt, ok = buildColumnarFTSPrefilter(ctx, newScalar(ast.LogicOr, ngram, unsupported), table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)

		rebuilt, ok = buildColumnarFTSPrefilter(ctx, newScalar(ast.UnaryNot, ngram), table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("expressions containing FTS require a wholly lossless rewrite", func(t *testing.T) {
		match := newFTSMatchWord(expression.NewStrConst("hello"), exactColumn)
		ngram := newScalar(ast.Like, textColumn, expression.NewStrConst("abcd"), expression.NewInt64Const('\\'))
		input := newScalar(ast.LogicOr, match, ngram)

		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
		rebuilt, ok = buildColumnarFTSPrefilter(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("match word rejects exact and resolves supported analyzers", func(t *testing.T) {
		query := expression.NewStrConst("hello")
		singleAnalyzerColumn := exactColumn.Clone().(*expression.Column)
		for _, explicit := range []bool{false, true} {
			input := newFTSMatchWord(query, singleAnalyzerColumn)
			if explicit {
				expression.SetFTSMatchWordOptions(input, &expression.FTSMatchWordOptions{
					Fields: []expression.FTSMatchWordFieldOption{{ParserName: "EXACT"}},
				})
			}
			rebuilt, ok, err := checkExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
			require.False(t, ok)
			require.Nil(t, rebuilt)
			require.ErrorContains(t, err, "does not support fields using the EXACT parser")
		}

		input := newFTSMatchWord(query, textColumn)
		expression.SetFTSMatchWordOptions(input, &expression.FTSMatchWordOptions{
			Fields: []expression.FTSMatchWordFieldOption{{ParserName: "NGRAM"}},
		})
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeNgramV1)
		require.Equal(t, "NGRAM", expression.GetFTSMatchWordOptions(input).Fields[0].ParserName)

		input = newFTSMatchWord(query, textColumn)
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeStandardV1)
	})

	t.Run("direct match word rejects enum exact fields", func(t *testing.T) {
		input := newFTSMatchWord(expression.NewStrConst("medium"), enumColumn)
		rebuilt, ok, err := checkExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)
		require.ErrorContains(t, err, "FTS_MATCH_WORD does not support fields using the EXACT parser")
	})

	t.Run("match word keeps parser selections scoped to field occurrences", func(t *testing.T) {
		input := newFTSMatchWord(expression.NewStrConst("hello"), textColumn, textColumn)
		expression.SetFTSMatchWordOptions(input, &expression.FTSMatchWordOptions{
			Fields: []expression.FTSMatchWordFieldOption{
				{ParserName: "STANDARD"},
				{ParserName: "NGRAM"},
			},
		})

		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		info := expression.InterpretFullTextSearchExpr(rebuilt)
		require.NotNil(t, info)
		require.Equal(t, []expression.QueryField{
			{ColID: textColumn.ID, IndexField: string(ast.FullTextParserTypeStandardV1)},
			{ColID: textColumn.ID, IndexField: string(ast.FullTextParserTypeNgramV1)},
		}, info.Clause.Fields)
		require.Equal(t, "STANDARD", expression.GetFTSMatchWordOptions(input).Fields[0].ParserName)
		require.Equal(t, "NGRAM", expression.GetFTSMatchWordOptions(input).Fields[1].ParserName)
	})

	t.Run("rejects uncovered columns and unstable or incompatible constants", func(t *testing.T) {
		cases := []expression.Expression{
			newScalar(ast.EQ, uncoveredColumn, expression.NewStrConst("hello")),
			newScalar(ast.EQ, exactColumn, expression.NewInt64Const(1)),
			newScalar(ast.EQ, exactColumn, &expression.Constant{Value: types.NewDatum(nil), RetType: types.NewFieldType(mysql.TypeNull)}),
		}
		for _, input := range cases {
			rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
			require.False(t, ok)
			require.Nil(t, rebuilt)
		}
	})

	t.Run("parameterized exact constants use the current execution value", func(t *testing.T) {
		parameter := &expression.Constant{
			Value:       types.NewStringDatum("hello"),
			RetType:     stringType.Clone(),
			ParamMarker: &expression.ParamMarker{},
		}
		input := newScalar(ast.EQ, exactColumn, expression.NewStrConst("placeholder"))
		input.GetArgs()[1] = parameter
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		require.NotNil(t, parameter.ParamMarker, "the source expression must remain parameterized")
		info := expression.InterpretFullTextSearchExpr(rebuilt)
		require.NotNil(t, info)
		require.Equal(t, "hello", info.Clause.Query)
		requireBoundParser(t, rebuilt, ast.FullTextParserTypeExactV1)

		first := expression.NewInt64Const(1)
		first.ParamMarker = &expression.ParamMarker{}
		second := expression.NewInt64Const(2)
		second.ParamMarker = &expression.ParamMarker{}
		input = newScalar(ast.In, numberColumn, expression.NewInt64Const(0), expression.NewInt64Const(1))
		input.GetArgs()[1] = first
		input.GetArgs()[2] = second
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, index)
		require.True(t, ok)
		queries := make([]any, 0, 2)
		var collectQueries func(expression.Expression)
		collectQueries = func(expr expression.Expression) {
			if match := expression.InterpretFullTextSearchExpr(expr); match != nil {
				queries = append(queries, match.Clause.Query)
				return
			}
			if scalar, ok := expr.(*expression.ScalarFunction); ok {
				for _, arg := range scalar.GetArgs() {
					collectQueries(arg)
				}
			}
		}
		collectQueries(rebuilt)
		require.ElementsMatch(t, []any{int64(1), int64(2)}, queries)

		enumInput := newScalar(ast.EQ, enumColumn, expression.NewStrConst("medium"))
		enumParameter, ok := enumInput.GetArgs()[1].(*expression.Constant)
		require.True(t, ok)
		enumParameter.ParamMarker = &expression.ParamMarker{}
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, enumInput, table, index)
		require.True(t, ok)
		require.Equal(t, uint64(2), expression.InterpretFullTextSearchExpr(rebuilt).Clause.Query)

		lossyParameter := &expression.Constant{
			Value:       types.NewDatum(1.5),
			RetType:     types.NewFieldType(mysql.TypeDouble),
			ParamMarker: &expression.ParamMarker{},
		}
		lossyInput := newScalar(ast.EQ, numberColumn, expression.NewInt64Const(0))
		lossyInput.GetArgs()[1] = lossyParameter
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(ctx, lossyInput, table, index)
		require.False(t, ok)
		require.Nil(t, rebuilt)

		nullParameter := expression.NewNull()
		nullParameter.ParamMarker = &expression.ParamMarker{}
		nullInput := newScalar(ast.EQ, numberColumn, expression.NewInt64Const(0))
		nullInput.GetArgs()[1] = nullParameter
		rebuilt, ok = CheckExprCoveredByColumnarFTSAndRebuild(
			ctx, nullInput, table, index,
		)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})

	t.Run("legacy metadata is still restricted to declared index columns", func(t *testing.T) {
		legacy := &model.IndexInfo{
			ID:           32,
			Name:         ast.NewCIStr("legacy_fts"),
			State:        model.StatePublic,
			Columns:      []*model.IndexColumn{{Name: exactInfo.Name, Offset: 0}},
			FullTextInfo: &ast.FullTextIndexInfo{ParserType: ast.FullTextParserTypeStandardV1},
		}
		input := newFTSMatchWord(expression.NewStrConst("hello"), uncoveredColumn)
		rebuilt, ok := CheckExprCoveredByColumnarFTSAndRebuild(ctx, input, table, legacy)
		require.False(t, ok)
		require.Nil(t, rebuilt)
	})
}

func TestFTSExactQueryValueCanonicalization(t *testing.T) {
	setupStarterFTSTest(t)
	location := time.FixedZone("UTC-07", -7*60*60)
	ctx := exprstatic.NewExprContext(exprstatic.WithEvalCtx(exprstatic.NewEvalContext(
		exprstatic.WithLocation(location),
	)))
	column := func(tp byte, flags uint) *expression.Column {
		fieldType := types.NewFieldType(tp)
		fieldType.SetFlag(flags)
		return &expression.Column{ID: 1, UniqueID: 1, RetType: fieldType}
	}
	fractionalColumn := func(tp byte) *expression.Column {
		col := column(tp, 0)
		col.RetType.SetDecimal(6)
		return col
	}
	bitColumn := column(mysql.TypeBit, 0)
	bitColumn.RetType.SetFlen(8)
	enumColumn := column(mysql.TypeEnum, 0)
	enumColumn.RetType.SetCharset(charset.CharsetUTF8MB4)
	enumColumn.RetType.SetCollate("utf8mb4_general_ci")
	enumColumn.RetType.SetElems([]string{"small", "medium", "large"})
	numericEnumColumn := enumColumn.Clone().(*expression.Column)
	numericEnumColumn.RetType = numericEnumColumn.RetType.Clone()
	numericEnumColumn.RetType.AddFlag(mysql.EnumSetAsIntFlag)
	constant := func(value types.Datum, fieldType *types.FieldType) *expression.Constant {
		return &expression.Constant{Value: value, RetType: fieldType}
	}

	date, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13", mysql.TypeDate, 0)
	require.NoError(t, err)
	datePacked, err := date.ToPackedUint()
	require.NoError(t, err)

	datetime, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13 12:34:56.123456", mysql.TypeDatetime, 6)
	require.NoError(t, err)
	datetimePacked, err := datetime.ToPackedUint()
	require.NoError(t, err)

	timestamp, err := types.ParseTime(types.StrictContext.WithLocation(location), "2026-08-13 12:34:56.123456", mysql.TypeTimestamp, 6)
	require.NoError(t, err)
	timestampUTC := timestamp
	require.NoError(t, timestampUTC.ConvertTimeZone(location, time.UTC))
	timestampPacked, err := timestampUTC.ToPackedUint()
	require.NoError(t, err)

	duration, _, err := types.ParseDuration(types.StrictContext.WithLocation(location), "-12:34:56.123456", 6)
	require.NoError(t, err)

	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.AddFlag(mysql.UnsignedFlag)
	testCases := []struct {
		name     string
		column   *expression.Column
		constant *expression.Constant
		expected any
	}{
		{name: "signed minimum", column: column(mysql.TypeLonglong, 0), constant: expression.NewInt64Const(math.MinInt64), expected: int64(math.MinInt64)},
		{name: "unsigned maximum", column: column(mysql.TypeLonglong, mysql.UnsignedFlag), constant: expression.NewUInt64ConstWithFieldType(math.MaxUint64, unsignedType), expected: uint64(math.MaxUint64)},
		{name: "boolean", column: column(mysql.TypeTiny, mysql.IsBooleanFlag), constant: expression.NewInt64Const(1), expected: int64(1)},
		{name: "bit", column: bitColumn, constant: expression.NewInt64Const(255), expected: uint64(255)},
		{name: "year", column: column(mysql.TypeYear, 0), constant: expression.NewInt64Const(2026), expected: uint64(2026)},
		{name: "exactly representable float", column: column(mysql.TypeFloat, 0), constant: constant(types.NewDatum(0.5), types.NewFieldType(mysql.TypeDouble)), expected: float64(0.5)},
		{name: "double", column: column(mysql.TypeDouble, 0), constant: constant(types.NewDatum(0.1), types.NewFieldType(mysql.TypeDouble)), expected: float64(0.1)},
		{name: "date string", column: column(mysql.TypeDate, 0), constant: expression.NewStrConst("2026-08-13"), expected: datePacked},
		{name: "datetime string", column: fractionalColumn(mysql.TypeDatetime), constant: expression.NewStrConst("2026-08-13 12:34:56.123456"), expected: datetimePacked},
		{name: "timestamp string uses UTC storage encoding", column: fractionalColumn(mysql.TypeTimestamp), constant: expression.NewStrConst("2026-08-13 12:34:56.123456"), expected: timestampPacked},
		{name: "duration string", column: fractionalColumn(mysql.TypeDuration), constant: expression.NewStrConst("-12:34:56.123456"), expected: int64(duration.Duration)},
		{name: "enum normalized constant", column: numericEnumColumn, constant: constant(types.NewMysqlEnumDatum(types.Enum{Name: "large", Value: 3}), enumColumn.RetType.Clone()), expected: uint64(3)},
		{name: "character string", column: column(mysql.TypeVarchar, 0), constant: expression.NewStrConst("TiDB"), expected: "TiDB"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, ok := ftsExactQueryValue(ctx, testCase.column, testCase.constant, testCase.column.RetType.GetCollate())
			require.True(t, ok)
			require.Equal(t, testCase.expected, actual)
		})
	}

	binaryString := column(mysql.TypeVarchar, 0)
	binaryString.RetType.SetCharset(charset.CharsetBin)
	binaryString.RetType.SetCollate(charset.CollationBin)
	parameterized := expression.NewInt64Const(1)
	parameterized.ParamMarker = &expression.ParamMarker{}
	deferred := expression.NewInt64Const(1)
	deferred.DeferredExpr = expression.NewInt64Const(1)
	for _, testCase := range []struct {
		name     string
		column   *expression.Column
		constant *expression.Constant
	}{
		{name: "negative to unsigned", column: column(mysql.TypeLonglong, mysql.UnsignedFlag), constant: expression.NewInt64Const(-1)},
		{name: "fractional to integer", column: column(mysql.TypeLonglong, 0), constant: constant(types.NewDatum(1.5), types.NewFieldType(mysql.TypeDouble))},
		{name: "lossy float", column: column(mysql.TypeFloat, 0), constant: constant(types.NewDatum(0.1), types.NewFieldType(mysql.TypeDouble))},
		{name: "date with nonzero time", column: column(mysql.TypeDate, 0), constant: expression.NewStrConst("2026-08-13 12:00:00")},
		{name: "datetime beyond column precision", column: column(mysql.TypeDatetime, 0), constant: expression.NewStrConst("2026-08-13 12:34:56.4")},
		{name: "timestamp beyond column precision", column: column(mysql.TypeTimestamp, 0), constant: expression.NewStrConst("2026-08-13 12:34:56.4")},
		{name: "duration beyond column precision", column: column(mysql.TypeDuration, 0), constant: expression.NewStrConst("12:34:56.4")},
		{name: "raw BIT string storage coercion", column: bitColumn, constant: expression.NewStrConst("1")},
		{name: "enum negative ordinal", column: numericEnumColumn, constant: expression.NewInt64Const(-1)},
		{name: "invalid date", column: column(mysql.TypeDate, 0), constant: expression.NewStrConst("not-a-date")},
		{name: "binary string", column: binaryString, constant: expression.NewStrConst("TiDB")},
		{name: "numeric to string", column: column(mysql.TypeVarchar, 0), constant: expression.NewInt64Const(1)},
		{name: "nan", column: column(mysql.TypeDouble, 0), constant: constant(types.NewDatum(math.NaN()), types.NewFieldType(mysql.TypeDouble))},
		{name: "null", column: column(mysql.TypeLonglong, 0), constant: expression.NewNull()},
		{name: "unmaterialized parameter", column: column(mysql.TypeLonglong, 0), constant: parameterized},
		{name: "deferred", column: column(mysql.TypeLonglong, 0), constant: deferred},
		{name: "unsupported decimal column", column: column(mysql.TypeNewDecimal, 0), constant: expression.NewInt64Const(1)},
	} {
		t.Run("rejects "+testCase.name, func(t *testing.T) {
			actual, ok := ftsExactQueryValue(ctx, testCase.column, testCase.constant, testCase.column.RetType.GetCollate())
			require.False(t, ok)
			require.Nil(t, actual)
		})
	}

	t.Run("rejects enum labels ambiguous under comparison collation", func(t *testing.T) {
		binaryEnumColumn := enumColumn.Clone().(*expression.Column)
		binaryEnumColumn.RetType = binaryEnumColumn.RetType.Clone()
		binaryEnumColumn.RetType.SetCollate(charset.CollationUTF8MB4)
		binaryEnumColumn.RetType.SetElems([]string{"a", "A"})
		actual, ok := ftsExactQueryValue(
			ctx, binaryEnumColumn, expression.NewStrConst("a"), "utf8mb4_general_ci",
		)
		require.False(t, ok)
		require.Nil(t, actual)
	})
}

func setupStarterFTSTest(t *testing.T) {
	t.Helper()

	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})
}
