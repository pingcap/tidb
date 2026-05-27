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
			name:   "plain standard term that analyzes to multiple tokens is no-match",
			query:  "hello-world",
			cols:   []ColumnInput{{Text: "hello world"}},
			expect: false,
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

func TestCompileBooleanQueryNgram(t *testing.T) {
	config := ngramConfigForTest()
	require.True(t, matchQueryForTest(t, config, "abc", []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "abc", []ColumnInput{{Text: "acb"}}))
	require.True(t, matchQueryForTest(t, config, `"abc"`, []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "a*", []ColumnInput{{Text: "abc"}}))
	require.True(t, matchQueryForTest(t, config, "ab*", []ColumnInput{{Text: "abc"}}))
	require.False(t, matchQueryForTest(t, config, "abc*", []ColumnInput{{Text: "abc"}}))
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
