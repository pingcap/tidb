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

package localfts

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestPreserveUnderscoreTokenize(t *testing.T) {
	tokens := PreserveUnderscoreTokenize("abc_def,ghi 你好-world")
	require.Equal(t, []Token{
		{Text: "abc_def", Position: 0},
		{Text: "ghi", Position: 1},
		{Text: "你好", Position: 2},
		{Text: "world", Position: 3},
	}, tokens)
}

func TestAnalyzeStandardV1(t *testing.T) {
	sctx := newFulltextTestContext(t)

	// Stopwords are enabled by default, so "the" is dropped by the InnoDB
	// default list. Its position is not reused: the remaining tokens keep the
	// ordinals they had in the original stream, which is what phrase matching
	// relies on.
	tokens, err := AnalyzeStandardV1(sctx, "the foo_bar, a 好")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "foo_bar", Position: 1},
	}, tokens)

	// Turning stopwords off keeps the same word, so the two settings produce
	// different token streams for identical input.
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbFtEnableStopword, variable.Off))
	tokens, err = AnalyzeStandardV1(sctx, "the foo_bar, a 好")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "the", Position: 0},
		{Text: "foo_bar", Position: 1},
	}, tokens)

	// With TiDB's default utf8mb4_bin server collation, stopword lookup is
	// case-sensitive. "The" is therefore not the built-in stopword "the".
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbFtEnableStopword, variable.On))
	tokens, err = AnalyzeStandardV1(sctx, "The cat")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "The", Position: 0},
		{Text: "cat", Position: 1},
	}, tokens)
}

func TestAnalyzeStandardV1StopwordsUseServerCollation(t *testing.T) {
	analyzer, err := GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		Collation:              "utf8mb4_bin",
		StopwordCollation:      "utf8mb4_general_ci",
		InnodbFtMinTokenSize:   3,
		InnodbFtMaxTokenSize:   84,
		InnodbFtEnableStopword: true,
	})
	require.NoError(t, err)

	tokens, err := analyzer.Analyze("thé")
	require.NoError(t, err)
	require.Empty(t, tokens, "the server collation considers thé equal to the stopword the")

	// Stopword lookup follows collation_server, not the MATCH column collation.
	analyzer, err = GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		Collation:              "utf8mb4_general_ci",
		StopwordCollation:      "utf8mb4_bin",
		InnodbFtMinTokenSize:   3,
		InnodbFtMaxTokenSize:   84,
		InnodbFtEnableStopword: true,
	})
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("The thé the")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "The", Position: 0},
		{Text: "thé", Position: 1},
	}, tokens, "the binary server collation is case- and accent-sensitive for stopwords")

	// Stopword filtering must see the source spelling even when there is no
	// MATCH-column collator and the remaining tokens are normalized to lower
	// case for TiDB's legacy binary-comparison path.
	analyzer, err = GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		StopwordCollation:      "utf8mb4_bin",
		InnodbFtMinTokenSize:   3,
		InnodbFtMaxTokenSize:   84,
		InnodbFtEnableStopword: true,
	})
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("The")
	require.NoError(t, err)
	require.Equal(t, []Token{{Text: "the", Position: 0}}, tokens,
		"case-sensitive stopword filtering happens before token normalization")
}

// TestDefaultInnodbStopwordList guards the transcription of MySQL's
// fts_default_stopword array. A wrong entry silently changes which rows a query
// matches, so the contents are pinned rather than only their effect.
func TestDefaultInnodbStopwordList(t *testing.T) {
	set := stopwordSetFromConfig(AnalyzerConfig{InnodbFtEnableStopword: true})
	require.Len(t, set, 35, "35 distinct words; the source array lists \"the\" twice")

	for _, word := range []string{"a", "the", "www", "und", "la", "com", "how", "who"} {
		require.Contains(t, set, word)
	}
	for _, word := range []string{"cat", "database", "tidb", "mysql", "storage"} {
		require.NotContains(t, set, word, "%q is not an InnoDB stop word", word)
	}

	// An explicit list replaces the default rather than adding to it.
	explicit := stopwordSetFromConfig(AnalyzerConfig{
		InnodbFtEnableStopword: true,
		Stopwords:              []string{"cat"},
	})
	require.Len(t, explicit, 1)
	require.Contains(t, explicit, "cat")
	require.NotContains(t, explicit, "the")

	require.Nil(t, stopwordSetFromConfig(AnalyzerConfig{InnodbFtEnableStopword: false}))
}

func TestAnalyzeStandardV1ReadsTokenSizesFromSessionContext(t *testing.T) {
	sctx := newFulltextTestContext(t)
	setGlobalSysVar(t, sctx, variable.InnodbFtMinTokenSize, "1")
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbFtEnableStopword, variable.Off))

	tokens, err := AnalyzeStandardV1(sctx, "A 好 xy")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "A", Position: 0},
		{Text: "好", Position: 1},
		{Text: "xy", Position: 2},
	}, tokens)
}

func TestAnalyzeNgramV1(t *testing.T) {
	sctx := newFulltextTestContext(t)

	tokens, err := AnalyzeNgramV1(sctx, "Hi世界 foo-bar")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "世界", Position: 2},
		{Text: "fo", Position: 3},
		{Text: "oo", Position: 4},
	}, tokens)
}

func TestAnalyzeNgramV1ShortTokenAdvancesPositionBase(t *testing.T) {
	sctx := newFulltextTestContext(t)

	tokens, err := AnalyzeNgramV1(sctx, "abc x 好 y A_b 中z")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "bc", Position: 1},
		{Text: "A_", Position: 4},
		{Text: "_b", Position: 5},
		{Text: "中z", Position: 6},
	}, tokens)
}

func TestUTF8CharSpansInvalidUTF8(t *testing.T) {
	text := string([]byte{'a', 0xff, 'b'})
	require.Equal(t, []charSpan{
		{byteStart: 0, byteEnd: 1},
		{byteStart: 1, byteEnd: 2},
		{byteStart: 2, byteEnd: 3},
	}, utf8CharSpans(text))
}

func TestAnalyzeNgramV1ReadsTokenSizeFromSessionContext(t *testing.T) {
	sctx := newFulltextTestContext(t)
	setGlobalSysVar(t, sctx, variable.NgramTokenSize, "3")

	tokens, err := AnalyzeNgramV1(sctx, "abcd")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "bcd", Position: 1},
	}, tokens)
}

func TestAnalyzeNgramV1Stopwords(t *testing.T) {
	config := AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		InnodbFtEnableStopword: true,
		NgramTokenSize:         2,
	}
	analyzer, err := GetAnalyzer(config)
	require.NoError(t, err)

	// "cool" has no stopwords. "the" is longer than the configured ngram
	// size, so its grams survive.
	tokens, err := analyzer.Analyze("cool the")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "co", Position: 0},
		{Text: "oo", Position: 1},
		{Text: "ol", Position: 2},
		{Text: "th", Position: 3},
		{Text: "he", Position: 4},
	}, tokens)
	tokens, err = analyzer.Analyze("an")
	require.NoError(t, err)
	require.Empty(t, tokens, "a stopword whose length equals ngram_token_size is filtered")

	config.NgramTokenSize = 3
	analyzer, err = GetAnalyzer(config)
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("other")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "oth", Position: 0},
		{Text: "her", Position: 2},
	}, tokens, "the gram containing the default stopword must be removed without renumbering")

	config.InnodbFtEnableStopword = false
	analyzer, err = GetAnalyzer(config)
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("other")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "oth", Position: 0},
		{Text: "the", Position: 1},
		{Text: "her", Position: 2},
	}, tokens)
}

func TestAnalyzeNgramV1StopwordsUseCollation(t *testing.T) {
	analyzer, err := GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		Collation:              "utf8mb4_bin",
		StopwordCollation:      "utf8mb4_general_ci",
		InnodbFtEnableStopword: true,
		NgramTokenSize:         2,
	})
	require.NoError(t, err)

	tokens, err := analyzer.Analyze("xá")
	require.NoError(t, err)
	require.Empty(t, tokens,
		"collation_server treats 'á' as containing stopword 'a'")

	analyzer, err = GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		Collation:              "utf8mb4_general_ci",
		StopwordCollation:      "utf8mb4_bin",
		InnodbFtEnableStopword: true,
		NgramTokenSize:         2,
	})
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("xá")
	require.NoError(t, err)
	require.Equal(t, []Token{{Text: "xá", Position: 0}}, tokens,
		"the MATCH column collation must not override the binary server collation for stopword lookup")

	analyzer, err = GetAnalyzer(AnalyzerConfig{
		ParserType:             model.FullTextParserTypeNgramV1,
		StopwordCollation:      "utf8mb4_bin",
		InnodbFtEnableStopword: true,
		NgramTokenSize:         2,
	})
	require.NoError(t, err)
	tokens, err = analyzer.Analyze("XA")
	require.NoError(t, err)
	require.Equal(t, []Token{{Text: "xa", Position: 0}}, tokens,
		"binary stopword matching must inspect the original gram before case normalization")
}

func TestAnalyzeStandardV1CustomStopwords(t *testing.T) {
	tokens := analyzeStandardV1("foo the bar", parserInfo{
		innodbFtMinTokenSize: 3,
		innodbFtMaxTokenSize: 84,
		stopwords:            stopwordSet("foo"),
	})
	require.Equal(t, []Token{
		{Text: "the", Position: 1},
		{Text: "bar", Position: 2},
	}, tokens)
}

func newFulltextTestContext(t *testing.T) *mock.Context {
	sctx := mock.NewContext()
	globalAccessor := variable.NewMockGlobalAccessor4Tests()
	globalAccessor.SessionVars = sctx.GetSessionVars()
	sctx.GetSessionVars().GlobalVarsAccessor = globalAccessor
	return sctx
}

func setGlobalSysVar(t *testing.T, sctx *mock.Context, name, value string) {
	globalAccessor, ok := sctx.GetSessionVars().GlobalVarsAccessor.(*variable.MockGlobalAccessor)
	require.True(t, ok)
	require.NoError(t, globalAccessor.SetGlobalSysVarOnly(context.Background(), name, value, false))
}
