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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestValidateFTSSearchStringForLikeFallback(t *testing.T) {
	naturalMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)
	booleanMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)

	tests := []struct {
		name     string
		text     string
		modifier ast.FulltextSearchModifier
		wantErr  bool
	}{
		// Natural-language mode: plain alphanumeric words only.
		{name: "natural empty", text: "", modifier: naturalMode, wantErr: false},
		{name: "natural whitespace only", text: " \t\n ", modifier: naturalMode, wantErr: false},
		{name: "natural single word", text: "MySQL", modifier: naturalMode, wantErr: false},
		{name: "natural multi word", text: "MySQL tutorial PostgreSQL", modifier: naturalMode, wantErr: false},
		{name: "natural alphanumeric mix", text: "abc123 mysql8", modifier: naturalMode, wantErr: false},
		{name: "natural rejects mid-word dash", text: "x-x", modifier: naturalMode, wantErr: true},
		{name: "natural rejects punctuation suffix", text: "MySQL,", modifier: naturalMode, wantErr: true},
		{name: "natural rejects + operator", text: "+word", modifier: naturalMode, wantErr: true},
		{name: "natural rejects - operator", text: "-word", modifier: naturalMode, wantErr: true},
		{name: "natural rejects quote", text: `"phrase"`, modifier: naturalMode, wantErr: true},
		{name: "natural rejects wildcard", text: "word*", modifier: naturalMode, wantErr: true},
		{name: "natural rejects percent", text: "100%", modifier: naturalMode, wantErr: true},
		{name: "natural rejects underscore", text: "test_file", modifier: naturalMode, wantErr: true},

		// Boolean mode: plain word, +word, -word with alphanumeric body only.
		{name: "boolean empty", text: "", modifier: booleanMode, wantErr: false},
		{name: "boolean plain word", text: "MySQL", modifier: booleanMode, wantErr: false},
		{name: "boolean required word", text: "+MySQL", modifier: booleanMode, wantErr: false},
		{name: "boolean excluded word", text: "-MySQL", modifier: booleanMode, wantErr: false},
		{name: "boolean mix", text: "+apple -cherry pie", modifier: booleanMode, wantErr: false},
		{name: "boolean rejects mid-word dash", text: "xx-yy", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects bare operator", text: "+", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects bare minus", text: "-", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects + after body", text: "x+y", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects wildcard", text: "word*", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects required wildcard", text: "+word*", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects relevance gt", text: ">word", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects relevance lt", text: "<word", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects relevance tilde", text: "~word", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects phrase", text: `"exact phrase"`, modifier: booleanMode, wantErr: true},
		{name: "boolean rejects required phrase", text: `+"required phrase"`, modifier: booleanMode, wantErr: true},
		{name: "boolean rejects grouping", text: "(word)", modifier: booleanMode, wantErr: true},
		{name: "boolean rejects percent", text: "+100%", modifier: booleanMode, wantErr: true},
		// Multi-byte UTF-8 word characters pass (matches isFTSWordByte > 127 case).
		{name: "natural utf8 word", text: "你好", modifier: naturalMode, wantErr: false},
		{name: "boolean utf8 word", text: "+你好", modifier: booleanMode, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFTSSearchStringForLikeFallback(tt.text, tt.modifier)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParseFTSBooleanSearchString covers the strict-subset inputs the boolean
// parser is expected to handle in production. Inputs outside the subset
// (phrases, wildcards, relevance modifiers, mid-word punctuation, etc.) are
// rejected upstream by ValidateFTSSearchStringForLikeFallback and therefore
// never reach this parser.
func TestParseFTSBooleanSearchString(t *testing.T) {
	tests := []struct {
		input    string
		expected []ftsSearchTerm
	}{
		{
			input: "+apple +pie",
			expected: []ftsSearchTerm{
				{word: "apple", isRequired: true},
				{word: "pie", isRequired: true},
			},
		},
		{
			input: "+apple -cherry",
			expected: []ftsSearchTerm{
				{word: "apple", isRequired: true},
				{word: "cherry", isExcluded: true},
			},
		},
		{
			input: "word1 word2 word3",
			expected: []ftsSearchTerm{
				{word: "word1"},
				{word: "word2"},
				{word: "word3"},
			},
		},
		{
			input: "word1\t\nword2",
			expected: []ftsSearchTerm{
				{word: "word1"},
				{word: "word2"},
			},
		},
		{
			input:    "",
			expected: nil,
		},
		{
			input:    "   \t\n  ",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseFTSBooleanSearchString(tt.input)
			require.Equal(t, len(tt.expected), len(result), "Number of terms should match")
			for i, expected := range tt.expected {
				require.Equal(t, expected.word, result[i].word, "Word should match")
				require.Equal(t, expected.isRequired, result[i].isRequired, "isRequired should match")
				require.Equal(t, expected.isExcluded, result[i].isExcluded, "isExcluded should match")
			}
		})
	}
}

func TestParseFTSSearchTerm(t *testing.T) {
	tests := []struct {
		input    string
		expected ftsSearchTerm
	}{
		{input: "+word", expected: ftsSearchTerm{word: "word", isRequired: true}},
		{input: "-word", expected: ftsSearchTerm{word: "word", isExcluded: true}},
		{input: "word", expected: ftsSearchTerm{word: "word"}},
		{input: "", expected: ftsSearchTerm{}},
		// Bare operator with no body (caller passes the result through; the
		// upstream validator rejects this case before the parser sees it).
		{input: "+", expected: ftsSearchTerm{word: "", isRequired: true}},
		{input: "-", expected: ftsSearchTerm{word: "", isExcluded: true}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseFTSSearchTerm(tt.input)
			require.Equal(t, tt.expected.word, result.word, "Word should match")
			require.Equal(t, tt.expected.isRequired, result.isRequired, "isRequired should match")
			require.Equal(t, tt.expected.isExcluded, result.isExcluded, "isExcluded should match")
		})
	}
}

func TestEscapeFTSLikePattern(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "normal text",
			expected: "normal text",
		},
		{
			input:    "100%",
			expected: "100\\%",
		},
		{
			input:    "test_file",
			expected: "test\\_file",
		},
		{
			input:    "path\\to\\file",
			expected: "path\\\\to\\\\file",
		},
		{
			input:    "mix_%_all",
			expected: "mix\\_\\%\\_all",
		},
		{
			input:    "\\%_",
			expected: "\\\\\\%\\_",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeFTSLikePattern(tt.input)
			require.Equal(t, tt.expected, result, "Escaped pattern should match")
		})
	}
}

// newFTSMatchAgainstForTest builds a real FTSMysqlMatchAgainst ScalarFunction
// suitable for exercising BuildFTSToILikeExpressionFromBuiltin. It mirrors
// the planner's matchAgainstToBuiltin flow: build via NewFunction with a
// string Constant for AGAINST and one or more string Columns for MATCH,
// then attach the modifier via SetFTSMysqlMatchAgainstModifier.
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

func TestBuildFTSToILikeExpressionFromBuiltin(t *testing.T) {
	ctx := mock.NewContext()
	naturalMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)

	t.Run("nil scalar function", func(t *testing.T) {
		_, err := BuildFTSToILikeExpressionFromBuiltin(ctx, nil)
		require.Error(t, err)
	})

	t.Run("wrong function name", func(t *testing.T) {
		// Construct a non-FTS ScalarFunction by reusing one we know exists.
		stringTp := types.NewFieldType(mysql.TypeVarchar)
		col := &Column{Index: 0, RetType: stringTp}
		other, err := NewFunction(ctx, ast.Length, types.NewFieldType(mysql.TypeLonglong), col)
		require.NoError(t, err)
		_, err = BuildFTSToILikeExpressionFromBuiltin(ctx, other.(*ScalarFunction))
		require.Error(t, err)
		require.Contains(t, err.Error(), ast.FTSMysqlMatchAgainst)
	})

	t.Run("single-column natural-language succeeds", func(t *testing.T) {
		sf := newFTSMatchAgainstForTest(t, ctx, "mysql", 1, naturalMode)
		expr, err := BuildFTSToILikeExpressionFromBuiltin(ctx, sf)
		require.NoError(t, err)
		require.NotNil(t, expr)
		// The result should be a scalar function (IFNULL(ILIKE,...)) — not the
		// untranslated FTS opaque builtin.
		resultSF, ok := expr.(*ScalarFunction)
		require.True(t, ok)
		require.NotEqual(t, ast.FTSMysqlMatchAgainst, resultSF.FuncName.L)
	})

	t.Run("multi-column rejected for selectivity substitution", func(t *testing.T) {
		// GetSelectivityByFilter declines expressions over more than one column,
		// so a multi-column substituted ILIKE would never improve the estimate.
		// BuildFTSToILikeExpressionFromBuiltin returns an error to keep that
		// path explicit; the selectivity caller's err-check then falls through
		// to the str-match default cleanly.
		sf := newFTSMatchAgainstForTest(t, ctx, "mysql", 2, naturalMode)
		_, err := BuildFTSToILikeExpressionFromBuiltin(ctx, sf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "multi-column")
	})

	t.Run("NULL search constant returns Constant(NULL)", func(t *testing.T) {
		// The builtin's getFunction allows NULL search constants explicitly
		// (builtin_fts.go:129); the substitution short-circuits to Constant(NULL)
		// rather than Constant(0) so it composes correctly under SQL three-valued
		// logic and matches the planner-side matchAgainstToLike NULL fast-path.
		stringTp := types.NewFieldType(mysql.TypeVarchar)
		nullArg := &Constant{Value: types.NewDatum(nil), RetType: stringTp}
		col := &Column{Index: 0, RetType: stringTp}
		fn, err := NewFunction(ctx, ast.FTSMysqlMatchAgainst, types.NewFieldType(mysql.TypeDouble), nullArg, col)
		require.NoError(t, err)
		sf := fn.(*ScalarFunction)
		require.NoError(t, SetFTSMysqlMatchAgainstModifier(sf, naturalMode))

		expr, err := BuildFTSToILikeExpressionFromBuiltin(ctx, sf)
		require.NoError(t, err)
		c, ok := expr.(*Constant)
		require.True(t, ok)
		require.True(t, c.Value.IsNull(), "expected Constant(NULL), got %v", c.Value)
	})

	t.Run("search string outside strict subset rejected", func(t *testing.T) {
		// Search string with mid-word `-` fails ValidateFTSSearchStringForLikeFallback
		// and propagates that rejection through BuildFTSToILikeExpression.
		sf := newFTSMatchAgainstForTest(t, ctx, "xx-yy", 1, naturalMode)
		_, err := BuildFTSToILikeExpressionFromBuiltin(ctx, sf)
		require.Error(t, err)
	})
}

func TestScalarExprSupportedByFlashRejectsNonDefaultFTSModifier(t *testing.T) {
	// The tipb pushdown protocol does not serialize the FTS modifier; TiFlash
	// reconstructs the signature with the default (natural-language) modifier.
	// scalarExprSupportedByFlash must therefore mark non-default-modifier
	// FTSMysqlMatchAgainst as NOT Flash-supported even though the function
	// name is generally Flash-pushdown-eligible. This is defense in depth on
	// top of the planner's modifier guard in matchAgainstToBuiltin.
	ctx := mock.NewContext()
	naturalMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)
	booleanMode := ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)
	queryExpansion := ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode | ast.FulltextSearchModifierWithQueryExpansion)

	cases := []struct {
		name     string
		modifier ast.FulltextSearchModifier
		want     bool
	}{
		{"natural-language mode is Flash-supported", naturalMode, true},
		{"boolean mode is not Flash-supported", booleanMode, false},
		{"with-query-expansion is not Flash-supported", queryExpansion, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sf := newFTSMatchAgainstForTest(t, ctx, "mysql", 1, tc.modifier)
			require.Equal(t, tc.want, scalarExprSupportedByFlash(ctx.GetEvalCtx(), sf))
		})
	}
}
