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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newFTSTokenizeForTest(t *testing.T, ctx BuildContext, parser string, minLen, maxLen int64, stopword bool) *ScalarFunction {
	t.Helper()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	stringTp.SetCollate(mysql.DefaultCollationName)
	stopwordVal := int64(0)
	if stopword {
		stopwordVal = 1
	}
	args := []Expression{
		&Column{Index: 0, RetType: stringTp},
		&Constant{Value: types.NewStringDatum(parser), RetType: stringTp},
		&Constant{Value: types.NewIntDatum(minLen), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&Constant{Value: types.NewIntDatum(maxLen), RetType: types.NewFieldType(mysql.TypeLonglong)},
		&Constant{Value: types.NewIntDatum(stopwordVal), RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	fn, err := NewFunction(ctx, ast.FTSTokenize, types.NewFieldType(mysql.TypeJSON), args...)
	require.NoError(t, err)
	sf, ok := fn.(*ScalarFunction)
	require.True(t, ok)
	return sf
}

func tokenizeToJSON(t *testing.T, ctx EvalContext, sf *ScalarFunction, text string) string {
	t.Helper()
	row := chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum(text)}).ToRow()
	v, isNull, err := sf.EvalJSON(ctx, row)
	require.NoError(t, err)
	require.False(t, isNull)
	return v.String()
}

func TestFTSTokenizeBasic(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSTokenizeForTest(t, ctx, "STANDARD", 3, 84, true)

	// Lowercased, and 'a' is dropped for being shorter than min_token_size.
	require.Equal(t, `["this", "tutorial", "provides", "basic", "mysql"]`,
		tokenizeToJSON(t, ctx, sf, "This tutorial provides a basic MySQL"))

	// Duplicates collapse, so the generated column value does not depend on
	// whether an index consumes it.
	require.Equal(t, `["mysql", "tutorial"]`,
		tokenizeToJSON(t, ctx, sf, "MySQL tutorial MySQL tutorial"))

	require.Equal(t, `[]`, tokenizeToJSON(t, ctx, sf, "ab cd"))
}

// TestFTSTokenizeIsDeterministic is the property that makes the function legal
// in a generated column: the result depends only on the arguments, never on
// session state.
func TestFTSTokenizeIsDeterministic(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSTokenizeForTest(t, ctx, "STANDARD", 3, 84, true)
	before := tokenizeToJSON(t, ctx, sf, "MySQL tutorial for the database")

	// Move the session-scoped analyzer variable away from the baked-in value.
	// The token-size variables are global-only, so this is the one a user could
	// realistically change mid-workload.
	require.NoError(t, ctx.GetSessionVars().SetSystemVar("innodb_ft_enable_stopword", "OFF"))

	require.Equal(t, before, tokenizeToJSON(t, ctx, sf, "MySQL tutorial for the database"))
}

func TestFTSTokenizeRespectsConfigArgs(t *testing.T) {
	ctx := mock.NewContext()

	// min_token_size=2 keeps the two-character token that the default drops.
	shortOK := newFTSTokenizeForTest(t, ctx, "STANDARD", 2, 84, true)
	require.Equal(t, `["ab", "cd"]`, tokenizeToJSON(t, ctx, shortOK, "ab cd"))

	// max_token_size truncates the long end.
	narrow := newFTSTokenizeForTest(t, ctx, "STANDARD", 3, 5, true)
	require.Equal(t, `["short"]`, tokenizeToJSON(t, ctx, narrow, "short elongated"))

	// The stopword flag is currently inert: stopwordSetFromConfig returns an
	// empty set unless an explicit word list is supplied, and no code path
	// populates one yet (see the TODO comment there about InnoDB stopword
	// tables). The argument is still carried in the schema so that enabling
	// stopwords later cannot silently reinterpret an existing index. Pin the
	// current behaviour so that change has to update this test deliberately.
	withStop := newFTSTokenizeForTest(t, ctx, "STANDARD", 1, 84, true)
	withoutStop := newFTSTokenizeForTest(t, ctx, "STANDARD", 1, 84, false)
	require.Equal(t, `["the", "database"]`, tokenizeToJSON(t, ctx, withStop, "the database"))
	require.Equal(t, `["the", "database"]`, tokenizeToJSON(t, ctx, withoutStop, "the database"))
}

func TestFTSTokenizeNullText(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSTokenizeForTest(t, ctx, "STANDARD", 3, 84, true)
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}).ToRow()

	// JSON null rather than SQL NULL: an empty array is skipped by the
	// multi-valued index, so a NULL text column would otherwise vanish from it.
	v, isNull, err := sf.EvalJSON(ctx, row)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "null", v.String())
}

func TestFTSTokenizeNgram(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSTokenizeForTest(t, ctx, "NGRAM", 2, 84, false)
	require.Equal(t, `["ab", "bc", "cd"]`, tokenizeToJSON(t, ctx, sf, "abcd"))
}

func TestFTSTokenizeRejectsBadConfig(t *testing.T) {
	ctx := mock.NewContext()
	stringTp := types.NewFieldType(mysql.TypeVarchar)
	intTp := types.NewFieldType(mysql.TypeLonglong)

	// Unknown parser is rejected at build time, not at the first row.
	_, err := NewFunction(ctx, ast.FTSTokenize, types.NewFieldType(mysql.TypeJSON),
		&Column{Index: 0, RetType: stringTp},
		&Constant{Value: types.NewStringDatum("NOSUCHPARSER"), RetType: stringTp},
		&Constant{Value: types.NewIntDatum(3), RetType: intTp},
		&Constant{Value: types.NewIntDatum(84), RetType: intTp},
		&Constant{Value: types.NewIntDatum(1), RetType: intTp},
	)
	require.ErrorContains(t, err, "NOSUCHPARSER")

	// A non-constant configuration could differ between the write that fills an
	// index entry and the read that consults it.
	_, err = NewFunction(ctx, ast.FTSTokenize, types.NewFieldType(mysql.TypeJSON),
		&Column{Index: 0, RetType: stringTp},
		&Column{Index: 1, RetType: stringTp},
		&Constant{Value: types.NewIntDatum(3), RetType: intTp},
		&Constant{Value: types.NewIntDatum(84), RetType: intTp},
		&Constant{Value: types.NewIntDatum(1), RetType: intTp},
	)
	require.ErrorContains(t, err, "non-constant parser")
}

// TestFTSTokenizeAnalyzerConfigRoundTrip covers the planner's ability to
// recover an index's analyzer snapshot from the expression it was built with.
func TestFTSTokenizeAnalyzerConfigRoundTrip(t *testing.T) {
	ctx := mock.NewContext()
	sf := newFTSTokenizeForTest(t, ctx, "STANDARD", 2, 40, false)

	textExpr, config, ok := FTSTokenizeAnalyzerConfig(ctx.GetEvalCtx(), sf)
	require.True(t, ok)
	require.Equal(t, model.FullTextParserTypeStandardV1, config.ParserType)
	require.Equal(t, 2, config.InnodbFtMinTokenSize)
	require.Equal(t, 40, config.InnodbFtMaxTokenSize)
	require.False(t, config.InnodbFtEnableStopword)
	col, ok := textExpr.(*Column)
	require.True(t, ok)
	require.Equal(t, 0, col.Index)

	// A non-FTS_TOKENIZE expression is not mistaken for one.
	_, _, ok = FTSTokenizeAnalyzerConfig(ctx.GetEvalCtx(), &Column{Index: 0, RetType: types.NewFieldType(mysql.TypeVarchar)})
	require.False(t, ok)
}
