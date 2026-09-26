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

package fulltext

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestGetAnalyzerWithExplicitConfig(t *testing.T) {
	standard, err := GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize:   1,
		InnodbFtMaxTokenSize:   84,
		InnodbFtEnableStopword: true,
		Stopwords:              []string{"the"},
		NgramTokenSize:         2,
	})
	require.NoError(t, err)
	tokens, err := standard.Analyze("The A ABC")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "a", Position: 1},
		{Text: "abc", Position: 2},
	}, tokens)

	ngram, err := GetAnalyzer(AnalyzerConfig{
		ParserType:           model.FullTextParserTypeNgramV1,
		InnodbFtMinTokenSize: 1,
		InnodbFtMaxTokenSize: 84,
		NgramTokenSize:       3,
	})
	require.NoError(t, err)
	tokens, err = ngram.Analyze("abcd")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "abc", Position: 0},
		{Text: "bcd", Position: 1},
	}, tokens)

	_, err = GetAnalyzer(AnalyzerConfig{ParserType: model.FullTextParserTypeMultilingualV1})
	require.ErrorContains(t, err, "unsupported fulltext parser type")
}

func TestBuildDocument(t *testing.T) {
	analyzer, err := GetAnalyzer(standardConfigForTest())
	require.NoError(t, err)
	doc, err := BuildDocument([]ColumnInput{
		{Text: "foo bar"},
		{IsNull: true},
		{Text: "foo-baz"},
	}, analyzer)
	require.NoError(t, err)

	require.Len(t, doc.Columns, 3)
	require.Equal(t, []Token{{Text: "foo", Position: 0}, {Text: "bar", Position: 1}}, doc.Columns[0].Tokens)
	require.Empty(t, doc.Columns[1].Tokens)
	require.Equal(t, []int{0}, doc.Columns[2].Positions["foo"])
	require.Equal(t, 2, doc.TokenFreq["foo"])
	require.True(t, doc.hasToken("baz"))
}

func TestCompileBooleanQueryStandard(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		cols   []ColumnInput
		expect bool
	}{
		{
			name:   "optional terms are OR filters",
			query:  "tidb mysql",
			cols:   []ColumnInput{{Text: "TiDB distributed SQL"}},
			expect: true,
		},
		{
			name:   "required and prohibited terms",
			query:  "+tidb -mysql",
			cols:   []ColumnInput{{Text: "TiDB storage"}},
			expect: true,
		},
		{
			name:   "prohibited term rejects document",
			query:  "+tidb -mysql",
			cols:   []ColumnInput{{Text: "TiDB MySQL"}},
			expect: false,
		},
		{
			name:   "only prohibited terms match nothing",
			query:  "-mysql",
			cols:   []ColumnInput{{Text: "TiDB storage"}},
			expect: false,
		},
		{
			name:   "optional term filtered by analyzer is ignored",
			query:  "a",
			cols:   []ColumnInput{{Text: "a"}},
			expect: false,
		},
		{
			name:   "required term filtered by analyzer is unsatisfiable",
			query:  "+a",
			cols:   []ColumnInput{{Text: "a"}},
			expect: false,
		},
		{
			// `-` is the prohibited operator, so this is "hello, not world"
			// rather than one term the analyzer splits. For the latter see
			// TestCompileBooleanQueryMultiTokenTerm, which uses `foo.bar`.
			name:   "prohibited term excludes a document that contains it",
			query:  "hello-world",
			cols:   []ColumnInput{{Text: "hello world"}},
			expect: false,
		},
		{
			name:   "prohibited term admits a document without it",
			query:  "hello-world",
			cols:   []ColumnInput{{Text: "hello"}},
			expect: true,
		},
		{
			name:   "phrase matches within one column",
			query:  `"hello world"`,
			cols:   []ColumnInput{{Text: "hello world"}},
			expect: true,
		},
		{
			name:   "phrase does not cross column boundary",
			query:  `"hello world"`,
			cols:   []ColumnInput{{Text: "hello"}, {Text: "world"}},
			expect: false,
		},
		{
			name:   "phrase respects analyzer position gaps",
			query:  `"foo a bar"`,
			cols:   []ColumnInput{{Text: "foo bar"}},
			expect: false,
		},
		{
			name:   "phrase gap can match with an intervening token",
			query:  `"foo a bar"`,
			cols:   []ColumnInput{{Text: "foo xx bar"}},
			expect: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, matchQueryForTest(t, standardConfigForTest(), tc.query, tc.cols))
		})
	}
}

func TestCompileBooleanQueryStandardPrefix(t *testing.T) {
	config := standardConfigForTest()
	require.True(t, matchQueryForTest(t, config, "ti*", []ColumnInput{{Text: "TiDB storage"}}))
	require.False(t, matchQueryForTest(t, config, "tidb-mysql*", []ColumnInput{{Text: "tidb mysql"}}))
	require.False(t, matchQueryForTest(t, config, "+tidb-mysql*", []ColumnInput{{Text: "tidb mysql"}}))
}

func TestCompileBooleanQueryRepeatedTokenPhraseMiss(t *testing.T) {
	const repetitions = 2048
	document := strings.TrimSpace(strings.Repeat("foo ", repetitions*2))
	phrase := `"` + strings.Repeat("foo ", repetitions) + `never"`
	require.False(t, matchQueryForTest(t, standardConfigForTest(), phrase, []ColumnInput{{Text: document}}))
}

func BenchmarkRepeatedTokenPhraseMiss(b *testing.B) {
	config := standardConfigForTest()
	analyzer, err := GetAnalyzer(config)
	require.NoError(b, err)
	for _, repetitions := range []int{128, 256, 512} {
		b.Run(fmt.Sprintf("query-%d-document-%d", repetitions, repetitions*2), func(b *testing.B) {
			query, err := CompileBooleanQuery(
				`"`+strings.Repeat("foo ", repetitions)+`never"`,
				config,
			)
			require.NoError(b, err)
			document := strings.TrimSpace(strings.Repeat("foo ", repetitions*2))
			doc, err := BuildDocument([]ColumnInput{{Text: document}}, analyzer)
			require.NoError(b, err)
			b.ResetTimer()
			for b.Loop() {
				query.Match(doc)
			}
		})
	}
}

func TestCompileBooleanQueryNgram(t *testing.T) {
	config := ngramConfigForTest()
	require.True(t, matchQueryForTest(t, config, "abc", []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "abc", []ColumnInput{{Text: "acb"}}))
	require.True(t, matchQueryForTest(t, config, `"abc"`, []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "a*", []ColumnInput{{Text: "abc"}}))
	require.True(t, matchQueryForTest(t, config, "ab*", []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "abc*", []ColumnInput{{Text: "abc"}}))
}

func TestCompiledQueryEstimationMetadata(t *testing.T) {
	simple, err := CompileBooleanQuery("+tidb", standardConfigForTest())
	require.NoError(t, err)
	require.False(t, simple.MatchesNothing())
	require.Zero(t, simple.DocumentMatchCost())
	term, ok := simple.SelectivityTerm()
	require.True(t, ok)
	require.Equal(t, "tidb", term)

	filtered, err := CompileBooleanQuery("+go", standardConfigForTest())
	require.NoError(t, err)
	require.True(t, filtered.MatchesNothing())
	_, ok = filtered.SelectivityTerm()
	require.False(t, ok)

	phrase, err := CompileBooleanQuery(`"tidb database"`, standardConfigForTest())
	require.NoError(t, err)
	require.Greater(t, phrase.MatchCost(), simple.MatchCost())
	require.Equal(t, float64(1), phrase.DocumentMatchCost())
	_, ok = phrase.SelectivityTerm()
	require.False(t, ok)

	sparsePhrase, err := CompileBooleanQuery(`"tidb a database"`, standardConfigForTest())
	require.NoError(t, err)
	require.Greater(t, sparsePhrase.DocumentMatchCost(), phrase.DocumentMatchCost())

	ngram, err := CompileBooleanQuery("tidb", ngramConfigForTest())
	require.NoError(t, err)
	_, ok = ngram.SelectivityTerm()
	require.False(t, ok)
}

func matchQueryForTest(t *testing.T, config AnalyzerConfig, query string, columns []ColumnInput) bool {
	compiled, err := CompileBooleanQuery(query, config)
	require.NoError(t, err)
	analyzer, err := GetAnalyzer(config)
	require.NoError(t, err)
	doc, err := BuildDocument(columns, analyzer)
	require.NoError(t, err)
	return compiled.Match(doc)
}

func standardConfigForTest() AnalyzerConfig {
	return AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 3,
		InnodbFtMaxTokenSize: 84,
		NgramTokenSize:       2,
	}
}

func ngramConfigForTest() AnalyzerConfig {
	return AnalyzerConfig{
		ParserType:           model.FullTextParserTypeNgramV1,
		InnodbFtMinTokenSize: 3,
		InnodbFtMaxTokenSize: 84,
		NgramTokenSize:       2,
	}
}

// TestCompileBooleanQueryMultiTokenTerm covers a boolean term the analyzer
// splits into several words, such as `foo.bar`. Each word is required: a
// document matching the original term contains all of them. Previously such a
// term was dropped, which made the whole query match no document at all.
func TestCompileBooleanQueryMultiTokenTerm(t *testing.T) {
	config := AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 1,
		InnodbFtMaxTokenSize: 84,
	}
	analyzer, err := GetAnalyzer(config)
	require.NoError(t, err)
	matches := func(search, text string) bool {
		query, err := CompileBooleanQuery(search, config)
		require.NoError(t, err)
		require.False(t, query.MatchesNothing(), "%q must not collapse to match-nothing", search)
		doc, err := BuildDocument([]ColumnInput{{Text: text}}, analyzer)
		require.NoError(t, err)
		return query.Match(doc)
	}

	require.True(t, matches("+foo.bar", "foo bar baz"))
	require.False(t, matches("+foo.bar", "foo only"), "every analyzed word is required")
	require.False(t, matches("+foo.bar", "bar only"))

	// A split term combines with other clauses as one required unit.
	require.True(t, matches("+foo.bar +baz", "foo bar baz"))
	require.False(t, matches("+foo.bar +baz", "foo bar"))

	// Optional position: the term still needs all of its words.
	require.True(t, matches("foo.bar qux", "qux alone"))
	require.True(t, matches("foo.bar qux", "foo bar"))

	// A term the analyzer removes entirely still constrains nothing.
	stopConfig := config
	stopConfig.InnodbFtMinTokenSize = 5
	query, err := CompileBooleanQuery("+abc", stopConfig)
	require.NoError(t, err)
	require.True(t, query.MatchesNothing(), "a required term that analyzes away can match nothing")
}

func mustCompileForIndexTerms(t *testing.T, search string) *Query {
	t.Helper()
	config := AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 3,
		InnodbFtMaxTokenSize: 84,
	}
	query, err := CompileBooleanQuery(search, config)
	require.NoError(t, err)
	return query
}

func TestQueryFilterTerms(t *testing.T) {
	for _, tc := range []struct {
		search   string
		ok       bool
		required []string
		optional []string
	}{
		// Required terms intersect.
		{search: "+distributed +sql", ok: true, required: []string{"distributed", "sql"}},
		// A prohibited term cannot narrow anything, but must not block the
		// required term from doing so.
		{search: "+distributed -mysql", ok: true, required: []string{"distributed"}},
		// Optional terms union, but only when every branch contributes one.
		{search: "distributed sql", ok: true, optional: []string{"distributed", "sql"}},
		// A phrase requires each of its tokens; adjacency is left to the residual.
		{search: `+"distributed sql"`, ok: true, required: []string{"distributed", "sql"}},
		// With allowPrefix=false a prefix cannot be expressed as a token lookup.
		// With a required term alongside it the query still narrows; alone it
		// does not.
		{search: "+distributed +sq*", ok: true, required: []string{"distributed"}},
		{search: "+sq*", ok: false},
		// One un-narrowable optional branch makes the whole union unsound: a
		// document could match via that branch alone.
		{search: "distributed sq*", ok: false},
		// Purely negative queries cannot generate candidates.
		{search: "-mysql", ok: false},
		// Duplicates collapse.
		{search: "+sql +sql", ok: true, required: []string{"sql"}},
	} {
		t.Run(tc.search, func(t *testing.T) {
			query := mustCompileForIndexTerms(t, tc.search)
			terms, ok := query.FilterTerms(false)
			require.Equal(t, tc.ok, ok)
			if !tc.ok {
				return
			}
			require.ElementsMatch(t, tc.required, terms.Required)
			require.ElementsMatch(t, tc.optional, terms.Optional)
			// Required and Optional are alternatives, never both.
			require.True(t, len(terms.Required) == 0 || len(terms.Optional) == 0)
		})
	}
}

// TestQueryIndexTermsAreSound is the correctness property the access path
// depends on: any document the query matches must satisfy the generated terms,
// so using them to pick candidates cannot lose a matching row.
func TestQueryFilterTermsAreSound(t *testing.T) {
	config := AnalyzerConfig{
		ParserType:           model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize: 3,
		InnodbFtMaxTokenSize: 84,
	}
	analyzer, err := GetAnalyzer(config)
	require.NoError(t, err)

	docs := []string{
		"distributed sql database",
		"relational storage engine",
		"sql is distributed here",
		"distributed",
		"mysql distributed sql",
		"",
	}
	searches := []string{
		"+distributed +sql", "+distributed -mysql", "distributed sql",
		`+"distributed sql"`, "+distributed +sq*", "+sql +sql", "+storage",
	}

	for _, search := range searches {
		query := mustCompileForIndexTerms(t, search)
		terms, ok := query.FilterTerms(false)
		if !ok {
			continue
		}
		for _, text := range docs {
			doc, err := BuildDocument([]ColumnInput{{Text: text}}, analyzer)
			require.NoError(t, err)
			if !query.Match(doc) {
				continue
			}
			for _, token := range terms.Required {
				require.True(t, doc.hasToken(token),
					"%q matches %q but lacks required index token %q", text, search, token)
			}
			if len(terms.Optional) > 0 {
				require.True(t, slices.ContainsFunc(terms.Optional, doc.hasToken),
					"%q matches %q but has none of the optional index tokens %v", text, search, terms.Optional)
			}
		}
	}
}

// TestQueryFilterTermsAllowPrefix covers the difference between the two
// consumers. A substring pre-filter can use a prefix term - any document
// matching `sq*` contains "sq" - while an exact token lookup cannot.
func TestQueryFilterTermsAllowPrefix(t *testing.T) {
	query := mustCompileForIndexTerms(t, "+sq*")

	_, ok := query.FilterTerms(false)
	require.False(t, ok, "a prefix alone cannot drive an exact token lookup")

	terms, ok := query.FilterTerms(true)
	require.True(t, ok, "a prefix alone can drive a substring pre-filter")
	require.Equal(t, []string{"sq"}, terms.Required)

	// An optional branch that only a prefix can serve is usable for substring
	// filtering, and not otherwise.
	mixed := mustCompileForIndexTerms(t, "distributed sq*")
	_, ok = mixed.FilterTerms(false)
	require.False(t, ok)
	terms, ok = mixed.FilterTerms(true)
	require.True(t, ok)
	require.ElementsMatch(t, []string{"distributed", "sq"}, terms.Optional)
}
