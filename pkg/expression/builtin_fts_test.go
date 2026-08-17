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

	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)

	// The prohibited term excludes the row even though the required one matches.
	v, isNull, err = sf.EvalReal(ctx, stringRow("TiDB MySQL"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)

	// A NULL column contributes no tokens rather than making the predicate NULL.
	v, isNull, err = sf.EvalReal(ctx, nullStringRow())
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

// TestFTSMysqlMatchAgainstLocalEvalWordBoundary covers the headline semantic
// difference from the ILIKE fallback, which matches "cat" inside "concatenate"
// because it can only test for a substring.
func TestFTSMysqlMatchAgainstLocalEvalWordBoundary(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+cat", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, stringRow("concatenate the categories"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)

	v, _, err = sf.EvalReal(ctx, stringRow("the cat sat"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)
}

// TestFTSMysqlMatchAgainstLocalEvalPhrase covers quoted phrases, which the
// ILIKE fallback cannot express at all: it degrades them to independent terms.
func TestFTSMysqlMatchAgainstLocalEvalPhrase(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, `"distributed sql"`, 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, stringRow("a distributed sql database"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	// Both words present but not adjacent, so the phrase must not match.
	v, _, err = sf.EvalReal(ctx, stringRow("sql that is distributed"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

func TestFTSMysqlMatchAgainstLocalEvalPrefix(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+data*", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, stringRow("the database layer"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	v, _, err = sf.EvalReal(ctx, stringRow("metadata only"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

// TestFTSMysqlMatchAgainstLocalEvalMultiColumn checks that a token found in any
// matched column satisfies the query, as MySQL treats the columns as one
// concatenated document.
func TestFTSMysqlMatchAgainstLocalEvalMultiColumn(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+storage", 2, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, twoStringRow("title text", "storage body"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	v, _, err = sf.EvalReal(ctx, twoStringRow("title text", "body text"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

// TestFTSMysqlMatchAgainstLocalEvalShortTokenFiltered checks that a term below
// innodb_ft_min_token_size is dropped by the analyzer. A query consisting only
// of such terms matches nothing, which the ILIKE fallback gets wrong by
// substring-matching them.
func TestFTSMysqlMatchAgainstLocalEvalShortTokenFiltered(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+ab", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("ab abc abcd"))
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

// TestFTSMysqlMatchAgainstLocalEvalRejectsNaturalLanguage checks that the
// no-score path refuses modifiers it cannot serve, rather than silently
// returning a 0/1 result where a relevance score is expected.
func TestFTSMysqlMatchAgainstLocalEvalRejectsNaturalLanguage(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifier(0))
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	_, _, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.ErrorContains(t, err, "IN BOOLEAN MODE")

	require.False(t, FTSModifierSupportedByLocalNoScore(ast.FulltextSearchModifier(0)))
	require.True(t, FTSModifierSupportedByLocalNoScore(ast.FulltextSearchModifierBooleanMode))
	require.False(t, FTSModifierSupportedByLocalNoScore(
		ast.FulltextSearchModifierBooleanMode|ast.FulltextSearchModifierWithQueryExpansion))
}

// TestFTSMysqlMatchAgainstLocalEvalPreparedSearchValueChanges checks that the
// compiled-query cache is keyed by search string, so re-executing a prepared
// statement with a new parameter does not reuse the previous query.
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
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

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
	sf := newFTSMatchAgainstForTest(t, ctx, "+tidb", 1, ast.FulltextSearchModifierBooleanMode)
	info := localEvalInfoForTest()
	info.SelectivityTerm = "tidb"
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, info))

	cloned := sf.Clone().(*ScalarFunction)
	clonedInfo, ok := FTSMysqlMatchAgainstLocalEvalInfo(cloned)
	require.True(t, ok)
	require.Equal(t, "tidb", clonedInfo.SelectivityTerm)

	// The clone carries its own copy: mutating it must not affect the original.
	clonedInfo.SelectivityTerm = "changed"
	originalInfo, ok := FTSMysqlMatchAgainstLocalEvalInfo(sf)
	require.True(t, ok)
	require.Equal(t, "tidb", originalInfo.SelectivityTerm)

	v, isNull, err := cloned.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)
}

// TestFTSMysqlMatchAgainstLocalEvalIgnoresStaleMatchNothing checks that the
// plan-time MatchNothing flag does not override the query actually in hand. The
// flag describes the search string seen when the plan was built, and a plan can
// be re-executed with a different one, so evaluation reads match-nothing from
// the compiled query instead.
func TestFTSMysqlMatchAgainstLocalEvalIgnoresStaleMatchNothing(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+tidb", 1, ast.FulltextSearchModifierBooleanMode)
	info := localEvalInfoForTest()
	info.MatchNothing = true // stale: "+tidb" does match documents
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, info))

	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v, "a stale flag must not suppress a real match")
}

// TestFTSMysqlMatchAgainstLocalEvalMatchNothingQuery covers a query that really
// matches nothing: every required term is removed by the analyzer, so no
// document can satisfy it.
func TestFTSMysqlMatchAgainstLocalEvalMatchNothingQuery(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+ab", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("ab abc abcd"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

// TestFTSMysqlMatchAgainstLocalEvalNotFlashSupported checks that a locally
// evaluated MATCH is never pushed to TiFlash, which cannot produce its result.
func TestFTSMysqlMatchAgainstLocalEvalNotFlashSupported(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))
	require.False(t, scalarExprSupportedByFlash(ctx.GetEvalCtx(), sf))
}

func localEvalInfoForTest() *FTSLocalEvalInfo {
	return &FTSLocalEvalInfo{
		AnalyzerConfig: fulltext.AnalyzerConfig{
			ParserType:           model.FullTextParserTypeStandardV1,
			InnodbFtMinTokenSize: 3,
			InnodbFtMaxTokenSize: 84,
			NgramTokenSize:       2,
		},
	}
}

func stringRow(s string) chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum(s)}).ToRow()
}

func twoStringRow(a, b string) chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{
		types.NewStringDatum(a),
		types.NewStringDatum(b),
	}).ToRow()
}

func nullStringRow() chunk.Row {
	return chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}).ToRow()
}
