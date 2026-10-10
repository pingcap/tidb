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
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newLocalMatchAgainstForTest(t *testing.T, ctx BuildContext, search string, numCols int, modifier ast.FulltextSearchModifier) *ScalarFunction {
	t.Helper()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	stringTp.SetCollate(mysql.DefaultCollationName)
	args := make([]Expression, 0, 1+numCols)
	args = append(args, &Constant{Value: types.NewStringDatum(search), RetType: stringTp})
	for i := range numCols {
		args = append(args, &Column{Index: i, RetType: stringTp})
	}
	fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), args...)
	require.NoError(t, err)
	sf, ok := fn.(*ScalarFunction)
	require.True(t, ok)
	require.NoError(t, SetMatchAgainstModifier(sf, modifier))
	return sf
}

func TestLocalMatchAgainst(t *testing.T) {
	ctx := mock.NewContext()
	booleanMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)

	sf := newLocalMatchAgainstForTest(t, ctx, "+tidb -mysql", 1, booleanMode)
	_, _, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.ErrorContains(t, err, "outside of fulltext index")

	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

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

func TestLocalMatchAgainstStateSurvivesCloneAndSubstitution(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+PostgreSQL", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))
	booleanQuery, err := fulltext.BuildLocalMatchAgainstBooleanQuery("+PostgreSQL", model.FullTextParserTypeStandardV1)
	require.NoError(t, err)
	require.NoError(t, SetLocalMatchAgainstTiFlashEvalInfo(sf, &LocalMatchAgainstTiFlashEvalInfo{BooleanQuery: booleanQuery}))

	t.Run("clone", func(t *testing.T) {
		cloned := sf.Clone().(*ScalarFunction)
		originalInfo, ok := GetLocalMatchAgainstEvalInfo(sf)
		require.True(t, ok)
		clonedInfo, ok := GetLocalMatchAgainstEvalInfo(cloned)
		require.True(t, ok)
		require.Equal(t, originalInfo, clonedInfo)
		require.NotSame(t, originalInfo, clonedInfo)
		originalTiFlashInfo, ok := GetLocalMatchAgainstTiFlashEvalInfo(sf)
		require.True(t, ok)
		clonedTiFlashInfo, ok := GetLocalMatchAgainstTiFlashEvalInfo(cloned)
		require.True(t, ok)
		require.Equal(t, originalTiFlashInfo, clonedTiFlashInfo)
		require.NotSame(t, originalTiFlashInfo.BooleanQuery, clonedTiFlashInfo.BooleanQuery)
		clonedTiFlashInfo.BooleanQuery.Nodes[0].Text = "changed"
		require.Equal(t, "PostgreSQL", originalTiFlashInfo.BooleanQuery.Nodes[0].GetText())

		v, isNull, err := cloned.EvalReal(ctx, stringRow("MySQL vs. PostgreSQL"))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(1), v)
	})

	t.Run("column substitute", func(t *testing.T) {
		multiColumnSF := newLocalMatchAgainstForTest(t, ctx, "+PostgreSQL", 2, ast.FulltextSearchModifierBooleanMode)
		require.NoError(t, SetLocalMatchAgainstEvalInfo(multiColumnSF, localEvalInfoForTest()))
		matchedColumns := []*Column{
			multiColumnSF.GetArgs()[1].(*Column),
			multiColumnSF.GetArgs()[2].(*Column),
		}
		replacements := make([]Expression, 0, len(matchedColumns))
		for i, matchedColumn := range matchedColumns {
			matchedColumn.UniqueID = int64(i + 1)
			replacement := matchedColumn.Clone().(*Column)
			replacement.UniqueID = int64(i + 3)
			replacements = append(replacements, replacement)
		}

		changed, failed, substituted := ColumnSubstituteImpl(
			ctx,
			multiColumnSF,
			NewSchema(matchedColumns...),
			replacements,
			false,
		)
		require.True(t, changed)
		require.False(t, failed)
		substitutedSF := substituted.(*ScalarFunction)
		_, ok := GetLocalMatchAgainstEvalInfo(substitutedSF)
		require.True(t, ok)
		for i, replacement := range replacements {
			require.Equal(t, replacement.(*Column).UniqueID, substitutedSF.GetArgs()[i+1].(*Column).UniqueID)
		}

		v, isNull, err := substitutedSF.EvalReal(ctx, twoStringRow("MySQL vs.", "PostgreSQL"))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(1), v)
	})

	t.Run("correlated column substitute", func(t *testing.T) {
		substituted, err := SubstituteCorCol2Constant(ctx, sf)
		require.NoError(t, err)
		substitutedSF := substituted.(*ScalarFunction)
		_, ok := GetLocalMatchAgainstEvalInfo(substitutedSF)
		require.True(t, ok)

		v, isNull, err := substitutedSF.EvalReal(ctx, stringRow("MySQL vs. PostgreSQL"))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(1), v)
	})
}

// TestLocalMatchAgainstWordBoundary covers the headline semantic
// difference from the ILIKE fallback, which matches "cat" inside "concatenate"
// because it can only test for a substring.
func TestLocalMatchAgainstWordBoundary(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+cat", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	// STANDARD keeps "category" as one token. ILIKE "%cat%" would match it,
	// which is incompatible with the Local MATCH result asserted here.
	v, isNull, err := sf.EvalReal(ctx, stringRow("category"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)

	v, _, err = sf.EvalReal(ctx, stringRow("concatenate the categories"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)

	v, _, err = sf.EvalReal(ctx, stringRow("the cat sat"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)
}

// TestLocalMatchAgainstPhrase covers quoted phrases, which the
// ILIKE fallback cannot express at all: it degrades them to independent terms.
func TestLocalMatchAgainstPhrase(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, `"distributed sql"`, 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, stringRow("a distributed sql database"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	// Both words present but not adjacent, so the phrase must not match.
	v, _, err = sf.EvalReal(ctx, stringRow("sql that is distributed"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

func TestLocalMatchAgainstPrefix(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+data*", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, stringRow("the database layer"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	v, _, err = sf.EvalReal(ctx, stringRow("metadata only"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

func TestLocalMatchAgainstCollation(t *testing.T) {
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(previous)

	ctx := mock.NewContext()
	newWithCollation := func(columnCollation string) *ScalarFunction {
		stringTp := types.NewFieldType(mysql.TypeVarchar)
		stringTp.SetCollate(columnCollation)
		search := &Constant{Value: types.NewStringDatum("+quick"), RetType: stringTp}
		column := &Column{Index: 0, RetType: stringTp}
		fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), search, column)
		require.NoError(t, err)
		sf := fn.(*ScalarFunction)
		require.NoError(t, SetMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
		info := localEvalInfoForTest()
		info.AnalyzerConfig.Collation = columnCollation
		require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, info))
		return sf
	}

	bin := newWithCollation("utf8mb4_bin")
	v, _, err := bin.EvalReal(ctx, stringRow("QUICK runner"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
	v, _, err = bin.EvalReal(ctx, stringRow("quick runner"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	ci := newWithCollation("utf8mb4_general_ci")
	v, _, err = ci.EvalReal(ctx, stringRow("QUICK runner"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)
}

func TestLocalMatchAgainstCollationMatrix(t *testing.T) {
	previous := collate.NewCollationEnabled()
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(previous)

	eval := func(columnCollation, search, document string) float64 {
		ctx := mock.NewContext()
		stringTp := types.NewFieldType(mysql.TypeVarchar)
		stringTp.SetCollate(columnCollation)
		searchExpr := &Constant{Value: types.NewStringDatum(search), RetType: stringTp}
		column := &Column{Index: 0, RetType: stringTp}
		fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), searchExpr, column)
		require.NoError(t, err)
		sf := fn.(*ScalarFunction)
		require.NoError(t, SetMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
		info := localEvalInfoForTest()
		info.AnalyzerConfig.Collation = columnCollation
		require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, info))

		value, isNull, err := sf.EvalReal(ctx, stringRow(document))
		require.NoError(t, err)
		require.False(t, isNull)
		return value
	}

	type collationExpectation struct {
		name         string
		termExpected []float64
		prefixExpect []float64
	}
	cases := []collationExpectation{
		{name: "utf8mb4_bin", termExpected: []float64{0, 0, 1}, prefixExpect: []float64{0, 1, 1}},
		{name: "utf8mb4_0900_bin", termExpected: []float64{0, 0, 1}, prefixExpect: []float64{0, 1, 1}},
		{name: "utf8mb4_general_ci", termExpected: []float64{1, 1, 1}, prefixExpect: []float64{1, 1, 1}},
		{name: "utf8mb4_unicode_ci", termExpected: []float64{1, 1, 1}, prefixExpect: []float64{1, 1, 1}},
		{name: "utf8mb4_0900_ai_ci", termExpected: []float64{1, 1, 1}, prefixExpect: []float64{1, 1, 1}},
	}
	documents := []string{"CAFE", "café", "cafe"}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			for i, document := range documents {
				require.Equal(t, testCase.termExpected[i], eval(testCase.name, "+cafe", document))
				require.Equal(t, testCase.prefixExpect[i], eval(testCase.name, "+caf*", document))
			}
		})
	}
}

// TestLocalMatchAgainstMultiColumn checks that a token found in any
// matched column satisfies the query, as MySQL treats the columns as one
// concatenated document.
func TestLocalMatchAgainstMultiColumn(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+storage", 2, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, _, err := sf.EvalReal(ctx, twoStringRow("title text", "storage body"))
	require.NoError(t, err)
	require.Equal(t, float64(1), v)

	v, _, err = sf.EvalReal(ctx, twoStringRow("title text", "body text"))
	require.NoError(t, err)
	require.Equal(t, float64(0), v)
}

// TestLocalMatchAgainstShortTokenFiltered checks that a term below
// innodb_ft_min_token_size is dropped by the analyzer. A query consisting only
// of such terms matches nothing, which the ILIKE fallback gets wrong by
// substring-matching them.
func TestLocalMatchAgainstShortTokenFiltered(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+ab", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("ab abc abcd"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

func TestLocalMatchAgainstNullSearch(t *testing.T) {
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	nullArg := &Constant{Value: types.NewDatum(nil), RetType: stringTp}
	col := &Column{Index: 0, RetType: stringTp}
	fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), nullArg, col)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)
	require.NoError(t, SetMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

// TestLocalMatchAgainstRejectsNaturalLanguage checks that the
// no-score path refuses modifiers it cannot serve, rather than silently
// returning a 0/1 result where a relevance score is expected.
func TestLocalMatchAgainstRejectsNaturalLanguage(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifier(0))
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	_, _, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.ErrorContains(t, err, "IN BOOLEAN MODE")

	require.False(t, MatchAgainstModifierSupportedByLocalNoScore(ast.FulltextSearchModifier(0)))
	require.True(t, MatchAgainstModifierSupportedByLocalNoScore(ast.FulltextSearchModifierBooleanMode))
	require.False(t, MatchAgainstModifierSupportedByLocalNoScore(
		ast.FulltextSearchModifierBooleanMode|ast.FulltextSearchModifierWithQueryExpansion))
}

// TestLocalMatchAgainstPreparedSearchValueChanges checks that the
// compiled-query cache is keyed by search string, so re-executing a prepared
// statement with a new parameter does not reuse the previous query.
func TestLocalMatchAgainstPreparedSearchValueChanges(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().PlanCacheParams.Reset()
	ctx.GetSessionVars().PlanCacheParams.Append(types.NewStringDatum("tidb"))
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	search := &Constant{RetType: stringTp, ParamMarker: &ParamMarker{order: 0}}
	col := &Column{Index: 0, RetType: stringTp}
	fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), search, col)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)
	require.NoError(t, SetMatchAgainstModifier(sf, ast.FulltextSearchModifierBooleanMode))
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

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

func TestLocalMatchAgainstCloneMetadata(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+tidb", 1, ast.FulltextSearchModifierBooleanMode)
	info := localEvalInfoForTest()
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, info))

	cloned := sf.Clone().(*ScalarFunction)
	clonedInfo, ok := GetLocalMatchAgainstEvalInfo(cloned)
	require.True(t, ok)
	require.Equal(t, info.AnalyzerConfig, clonedInfo.AnalyzerConfig)

	// The clone carries its own copy: mutating it must not affect the original.
	clonedInfo.AnalyzerConfig.NgramTokenSize++
	originalInfo, ok := GetLocalMatchAgainstEvalInfo(sf)
	require.True(t, ok)
	require.Equal(t, info.AnalyzerConfig.NgramTokenSize, originalInfo.AnalyzerConfig.NgramTokenSize)

	v, isNull, err := cloned.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), v)
}

// TestLocalMatchAgainstMatchNothingQuery covers a query that really
// matches nothing: every required term is removed by the analyzer, so no
// document can satisfy it.
func TestLocalMatchAgainstMatchNothingQuery(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "+ab", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))

	v, isNull, err := sf.EvalReal(ctx, stringRow("ab abc abcd"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

// TestLocalMatchAgainstNotFlashSupported checks that a locally
// evaluated MATCH is never pushed to TiFlash, which cannot produce its result.
func TestLocalMatchAgainstNotFlashSupported(t *testing.T) {
	ctx := mock.NewContext()
	sf := newLocalMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetLocalMatchAgainstEvalInfo(sf, localEvalInfoForTest()))
	require.False(t, scalarExprSupportedByFlash(ctx.GetEvalCtx(), sf))
}

func localEvalInfoForTest() *LocalMatchAgainstEvalInfo {
	return &LocalMatchAgainstEvalInfo{
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
