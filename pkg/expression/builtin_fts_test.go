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
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newFTSMatchAgainstForTest(t *testing.T, ctx BuildContext, search string, numCols int, modifier ast.FulltextSearchModifier) *ScalarFunction {
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
	require.NoError(t, SetFTSMysqlMatchAgainstModifier(sf, modifier))
	return sf
}

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
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))
	v, isNull, err = sf.EvalReal(ctx, stringRow("TiDB storage"))
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

// TestFTSMysqlMatchAgainstLocalEvalMatchNothing checks the short-circuit taken
// when the planner already proved the query matches no document.
func TestFTSMysqlMatchAgainstLocalEvalMatchNothing(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+tidb", 1, ast.FulltextSearchModifierBooleanMode)
	info := localEvalInfoForTest()
	info.MatchNothing = true
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, info))

	v, isNull, err := sf.EvalReal(ctx, stringRow("TiDB storage"))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(0), v)
}

// TestFTSMysqlMatchAgainstLocalEvalNotFlashSupported checks that a locally
// evaluated MATCH is never pushed to TiFlash, which cannot produce its result.
func TestFTSMysqlMatchAgainstLocalEvalNotFlashSupported(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	require.True(t, scalarExprSupportedByTiCI(ctx.GetEvalCtx(), sf))
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))
	require.False(t, scalarExprSupportedByFlash(ctx.GetEvalCtx(), sf))
	require.False(t, scalarExprSupportedByTiCI(ctx.GetEvalCtx(), sf))
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

func TestFTSMysqlMatchAgainstLocalRouting(t *testing.T) {
	ctx := mock.NewContext()
	local := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	native := newFTSMatchAgainstForTest(t, ctx, "mysql", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(local, localEvalInfoForTest()))
	cols := intset.NewFastIntSet(0)
	invertedCols := intset.NewFastIntSet()

	// Both are fulltext expressions, but only the native expression requires
	// TiCI and can be covered by its fulltext index.
	require.True(t, ContainsFullTextSearchFn(local))
	require.False(t, ContainsTiCIFullTextSearchFn(local))
	require.True(t, ContainsTiCIFullTextSearchFn(native))
	require.False(t, ExprCoveredByOneTiCIIndex(local, &cols, &cols, &invertedCols))
	require.True(t, ExprCoveredByOneTiCIIndex(native, &cols, &cols, &invertedCols))

	rewritten, changed, err := RewriteMySQLMatchAgainstRecursively(ctx, local, model.FullTextParserTypeStandardV1)
	require.NoError(t, err)
	require.False(t, changed)
	require.Same(t, local, rewritten)

	// A native sibling must still trigger TiCI validation. An OR containing
	// local evaluation cannot be pushed as a wholly covered TiCI predicate.
	for _, op := range []string{ast.LogicAnd, ast.LogicOr} {
		mixed := NewFunctionInternal(ctx, op, types.NewFieldType(mysql.TypeTiny), local, native)
		require.True(t, ContainsTiCIFullTextSearchFn(mixed))
		require.False(t, ExprCoveredByOneTiCIIndex(mixed, &cols, &cols, &invertedCols))
		rewritten, changed, err = RewriteMySQLMatchAgainstRecursively(ctx, mixed, model.FullTextParserTypeStandardV1)
		require.NoError(t, err)
		require.True(t, changed)
		args := rewritten.(*ScalarFunction).GetArgs()
		unwrapTruth := func(expr Expression) *ScalarFunction {
			sf := expr.(*ScalarFunction)
			if sf.FuncName.L == ast.IsTruthWithNull {
				return sf.GetArgs()[0].(*ScalarFunction)
			}
			return sf
		}
		require.Same(t, local, unwrapTruth(args[0]))
		require.Equal(t, ast.FTSMatchWord, unwrapTruth(args[1]).FuncName.L)
	}

	notLocal := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), local)
	require.True(t, ContainsFullTextSearchFn(notLocal))
	require.False(t, ContainsTiCIFullTextSearchFn(notLocal))
}

func TestFTSMysqlMatchAgainstStateSurvivesCloneAndSubstitution(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "+PostgreSQL", 1, ast.FulltextSearchModifierBooleanMode)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(sf, localEvalInfoForTest()))

	t.Run("clone", func(t *testing.T) {
		cloned := sf.Clone().(*ScalarFunction)
		originalInfo, ok := FTSMysqlMatchAgainstLocalEvalInfo(sf)
		require.True(t, ok)
		clonedInfo, ok := FTSMysqlMatchAgainstLocalEvalInfo(cloned)
		require.True(t, ok)
		require.Equal(t, originalInfo, clonedInfo)
		require.NotSame(t, originalInfo, clonedInfo)

		v, isNull, err := cloned.EvalReal(ctx, stringRow("MySQL vs. PostgreSQL"))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(1), v)
	})

	t.Run("column substitute", func(t *testing.T) {
		multiColumnSF := newFTSMatchAgainstForTest(t, ctx, "+PostgreSQL", 2, ast.FulltextSearchModifierBooleanMode)
		require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(multiColumnSF, localEvalInfoForTest()))
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
		_, ok := FTSMysqlMatchAgainstLocalEvalInfo(substitutedSF)
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
		correlatedSF := sf.Clone().(*ScalarFunction)
		search := types.NewStringDatum("+PostgreSQL")
		correlatedSF.GetArgs()[0] = &CorrelatedColumn{
			Column: Column{RetType: sf.GetArgs()[0].GetType(ctx)},
			Data:   &search,
		}
		substituted, err := SubstituteCorCol2Constant(ctx, correlatedSF)
		require.NoError(t, err)
		substitutedSF := substituted.(*ScalarFunction)
		_, ok := FTSMysqlMatchAgainstLocalEvalInfo(substitutedSF)
		require.True(t, ok)

		v, isNull, err := substitutedSF.EvalReal(ctx, stringRow("MySQL vs. PostgreSQL"))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, float64(1), v)
	})
}

func TestFTSMysqlMatchAgainstLocalStateEquality(t *testing.T) {
	ctx := mock.NewContext()
	native := newFTSMatchAgainstForTest(t, ctx, "tidb", 1, ast.FulltextSearchModifierBooleanMode)
	local := native.Clone().(*ScalarFunction)
	// Populate caches before attaching state to exercise setter invalidation.
	local.HashCode()
	local.CanonicalHashCode()
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(local, localEvalInfoForTest()))
	assertDifferent := func(a, b *ScalarFunction) {
		t.Helper()
		require.False(t, a.Equal(ctx, b))
		require.False(t, a.Equals(b))
		require.NotEqual(t, a.HashCode(), b.HashCode())
		require.NotEqual(t, a.CanonicalHashCode(), b.CanonicalHashCode())
		ha, hb := base.NewHashEqualer(), base.NewHashEqualer()
		a.Hash64(ha)
		b.Hash64(hb)
		require.NotEqual(t, ha.Sum64(), hb.Sum64())
	}
	assertDifferent(native, local)
	notNative := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), native).(*ScalarFunction)
	notLocal := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), local).(*ScalarFunction)
	assertDifferent(notNative, notLocal)
	for _, change := range []func(*fulltext.AnalyzerConfig){
		func(c *fulltext.AnalyzerConfig) { c.ParserType = model.FullTextParserTypeNgramV1 },
		func(c *fulltext.AnalyzerConfig) { c.InnodbFtMinTokenSize++ },
		func(c *fulltext.AnalyzerConfig) { c.InnodbFtMaxTokenSize++ },
		func(c *fulltext.AnalyzerConfig) { c.NgramTokenSize++ },
		func(c *fulltext.AnalyzerConfig) { c.InnodbFtEnableStopword = !c.InnodbFtEnableStopword },
		func(c *fulltext.AnalyzerConfig) { c.Stopwords = []string{"tidb"} },
	} {
		other := local.Clone().(*ScalarFunction)
		info := localEvalInfoForTest()
		change(&info.AnalyzerConfig)
		require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(other, info))
		assertDifferent(local, other)
	}
	cloned := local.Clone().(*ScalarFunction)
	require.True(t, local.Equal(ctx, cloned))
	require.True(t, local.Equals(cloned))
	require.Equal(t, local.HashCode(), cloned.HashCode())
	require.Equal(t, local.CanonicalHashCode(), cloned.CanonicalHashCode())
}

func TestFTSMysqlMatchAgainstNativeColumnSubstitution(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSMatchAgainstForTest(t, ctx, "hello", 1, ast.FulltextSearchModifierBooleanMode)
	col := sf.GetArgs()[1].(*Column)
	col.UniqueID = 1
	replacement := &Constant{Value: types.NewStringDatum("hello"), RetType: col.RetType}
	changed, failed, substituted := ColumnSubstituteImpl(ctx, sf, NewSchema(col), []Expression{replacement}, false)
	require.True(t, changed)
	require.True(t, failed)
	// Native MATCH still requires real columns; constant propagation must keep
	// the original expression so TiCI rewrite can safely construct its helper.
	require.Same(t, sf, substituted)
	require.Same(t, col, sf.GetArgs()[1])

	local := sf.Clone().(*ScalarFunction)
	require.NoError(t, SetFTSMysqlMatchAgainstLocalEvalInfo(local, localEvalInfoForTest()))
	changed, failed, substituted = ColumnSubstituteImpl(ctx, local, NewSchema(col), []Expression{replacement}, false)
	require.True(t, changed)
	require.False(t, failed)
	result, isNull, err := substituted.EvalReal(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, float64(1), result)
}
