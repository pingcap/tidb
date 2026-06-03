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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestFTSMysqlMatchAgainstLocalEval(t *testing.T) {
	ctx := mock.NewContext()
	booleanMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)

	sf := newFTSMatchAgainstForTest(t, ctx, "+tidb -mysql", 1, booleanMode)
	_, _, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.ErrorContains(t, err, "outside of fulltext index")

	require.NoError(t, SetFTSMysqlMatchAgainstUsage(sf, FTSMatchUsageDirectFilter))
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest(model.FullTextParserTypeStandardV1)))
	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)

	v, isNull, err = sf.EvalReal(ctx, stringRow("TiDB MySQL"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)

	v, isNull, err = sf.EvalReal(ctx, nullStringRow())
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

func TestFTSMysqlMatchAgainstLocalEvalNullSearch(t *testing.T) {
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	nullArg := &Constant{Value: types.NewDatum(nil), RetType: stringTp}
	col := &Column{Index: 0, RetType: stringTp}
	fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), nullArg, col)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)
	require.NoError(t, SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))

	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.True(t, isNull)
	require.Equal(t, float64(0), v)
}

func TestFTSMysqlMatchAgainstLocalEvalPreparedSearchValueChanges(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().PlanCacheParams.Reset()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewStringDatum("tidb"))
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	search := &Constant{RetType: stringTp, ParamMarker: &ParamMarker{order: 0}}
	col := &Column{Index: 0, RetType: stringTp}
	fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), search, col)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)
	require.NoError(t, SetFTSMysqlMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest(model.FullTextParserTypeStandardV1)))

	ctx.GetSessionVars().PlanCacheParams.Reset()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewStringDatum("tidb"))
	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)

	ctx.GetSessionVars().PlanCacheParams.Reset()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewStringDatum("mysql"))
	v, isNull, err = sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

func TestFTSMysqlMatchAgainstLocalEvalCloneMetadata(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstUsage(sf, FTSMatchUsageDirectFilter))
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest(model.FullTextParserTypeStandardV1)))

	cloned := sf.Clone().(*ScalarFunction)
	usage, ok := FTSMysqlMatchAgainstUsage(cloned)
	require.True(t, ok)
	require.Equal(t, FTSMatchUsageDirectFilter, usage)
	info, ok := FTSMysqlMatchAgainstLocalEvalInfo(cloned)
	require.True(t, ok)
	require.Equal(t, model.FullTextParserTypeStandardV1, info.ParserType)

	v, isNull, err := cloned.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)
}

func TestFTSMysqlMatchAgainstLocalEvalNotFlashSupported(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest(model.FullTextParserTypeStandardV1)))
	require.False(t, scalarExprSupportedByFlash(ctx.GetEvalCtx(), sf))
}

func localEvalInfoForTest(parserType model.FullTextParserType) *FTSLocalEvalInfo {
	return &FTSLocalEvalInfo{
		TableID:    1,
		IndexID:    2,
		ParserType: parserType,
		AnalyzerConfig: fulltext.AnalyzerConfig{
			ParserType:           parserType,
			InnodbFtMinTokenSize: 3,
			InnodbFtMaxTokenSize: 84,
			NgramTokenSize:       2,
		},
		ColumnIDs:       []int64{11},
		ColumnUniqueIDs: []int64{101},
		NoScore:         true,
	}
}

func stringRow(s string) chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum(s)}).ToRow()
}

func nullStringRow() chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}).ToRow()
}
