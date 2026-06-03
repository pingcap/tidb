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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestResolveProjectionRewritesTiCIFTSMatchAgainstToBM25ScoreColumn(t *testing.T) {
	t.Parallel()
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	stringTp.SetCollate(mysql.DefaultCollationName)

	contentCol := &expression.Column{ID: 1, UniqueID: 10, RetType: stringTp, OrigName: "content"}
	indexScan := (&physicalop.PhysicalIndexScan{
		DataSourceSchema: expression.NewSchema(contentCol),
		FtsQueryInfo:     &tipb.FTSQueryInfo{QueryType: tipb.FTSQueryType_FTSQueryTypeNoScore},
	}).Init(ctx, 0)
	indexScan.SetSchema(expression.NewSchema(contentCol))
	indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: indexScan}).Init(ctx, 0)
	require.Equal(t, 1, indexReader.Schema().Len())

	matchAgainst, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		&expression.Constant{Value: types.NewStringDatum("hello"), RetType: stringTp},
		contentCol,
	)
	require.NoError(t, err)
	sf := matchAgainst.(*expression.ScalarFunction)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))

	proj := (&physicalop.PhysicalProjection{Exprs: []expression.Expression{matchAgainst}}).Init(ctx, &property.StatsInfo{}, 0)
	proj.SetChildren(indexReader)
	require.NoError(t, resolveIndicesItself4PhysicalProjection(proj))

	col, ok := proj.Exprs[0].(*expression.Column)
	require.True(t, ok)
	require.Equal(t, model.ExtraBM25ScoreID, col.ID)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeWithScore, indexScan.FtsQueryInfo.QueryType)
	require.Equal(t, model.ExtraBM25ScoreID, indexScan.Schema().Columns[1].ID)
	require.Equal(t, 1, col.Index)
	require.Equal(t, 2, indexReader.Schema().Len())
	require.Equal(t, model.ExtraBM25ScoreID, indexReader.OutputColumns[1].ID)
	require.Equal(t, 1, indexReader.OutputColumns[1].Index)
}

func TestResolveProjectionContinuesAfterTiCIFTSScoreRewrite(t *testing.T) {
	t.Parallel()
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	stringTp.SetCollate(mysql.DefaultCollationName)
	scoreTp := types.NewFieldType(mysql.TypeDouble)
	scoreTp.SetFlag(mysql.NotNullFlag)
	idTp := types.NewFieldType(mysql.TypeLonglong)

	contentCol := &expression.Column{ID: 1, UniqueID: 10, RetType: stringTp, OrigName: "content"}
	idCol := &expression.Column{ID: 2, UniqueID: 20, RetType: idTp, OrigName: "id"}
	scoreCol := &expression.Column{ID: model.ExtraBM25ScoreID, UniqueID: 30, RetType: scoreTp, OrigName: model.ExtraBM25ScoreName.O}
	indexScan := (&physicalop.PhysicalIndexScan{
		DataSourceSchema: expression.NewSchema(contentCol, idCol),
		FtsQueryInfo:     &tipb.FTSQueryInfo{},
	}).Init(ctx, 0)
	indexScan.SetSchema(expression.NewSchema(contentCol, idCol, scoreCol))
	indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: indexScan}).Init(ctx, 0)

	matchAgainst, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		&expression.Constant{Value: types.NewStringDatum("hello"), RetType: stringTp},
		contentCol,
	)
	require.NoError(t, err)
	sf := matchAgainst.(*expression.ScalarFunction)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))

	proj := (&physicalop.PhysicalProjection{Exprs: []expression.Expression{matchAgainst, idCol}}).Init(ctx, &property.StatsInfo{}, 0)
	proj.SetChildren(indexReader)
	require.NoError(t, resolveIndicesItself4PhysicalProjection(proj))

	resolvedID, ok := proj.Exprs[1].(*expression.Column)
	require.True(t, ok)
	require.Equal(t, 1, resolvedID.Index)
}

func TestResolveProjectionLeavesTiCIFTSScoreUnsupportedIndexPlanUnchanged(t *testing.T) {
	t.Parallel()
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	stringTp.SetCollate(mysql.DefaultCollationName)

	contentCol := &expression.Column{ID: 1, UniqueID: 10, RetType: stringTp, OrigName: "content"}
	indexScan := (&physicalop.PhysicalIndexScan{
		DataSourceSchema: expression.NewSchema(contentCol),
		FtsQueryInfo:     &tipb.FTSQueryInfo{QueryType: tipb.FTSQueryType_FTSQueryTypeNoScore},
	}).Init(ctx, 0)
	indexScan.SetSchema(expression.NewSchema(contentCol))
	limit := (&physicalop.PhysicalLimit{}).Init(ctx, &property.StatsInfo{}, 0)
	limit.SetChildren(indexScan)
	limit.SetSchema(indexScan.Schema())
	indexReader := (&physicalop.PhysicalIndexReader{IndexPlan: limit}).Init(ctx, 0)

	matchAgainst, err := expression.NewFunction(
		ctx,
		ast.FTSMysqlMatchAgainst,
		types.NewFieldType(mysql.TypeDouble),
		&expression.Constant{Value: types.NewStringDatum("hello"), RetType: stringTp},
		contentCol,
	)
	require.NoError(t, err)
	sf := matchAgainst.(*expression.ScalarFunction)
	require.NoError(t, expression.SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))

	proj := (&physicalop.PhysicalProjection{Exprs: []expression.Expression{matchAgainst}}).Init(ctx, &property.StatsInfo{}, 0)
	proj.SetChildren(indexReader)
	require.NoError(t, resolveIndicesItself4PhysicalProjection(proj))

	sf, ok := proj.Exprs[0].(*expression.ScalarFunction)
	require.True(t, ok)
	require.Equal(t, ast.FTSMysqlMatchAgainst, sf.FuncName.L)
	require.Equal(t, tipb.FTSQueryType_FTSQueryTypeNoScore, indexScan.FtsQueryInfo.QueryType)
	require.Equal(t, 1, limit.Schema().Len())
}
