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
	tokens, err := AnalyzeStandardV1(sctx, "The foo_bar, a 好")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "foo_bar", Position: 1},
	}, tokens)

	// Turning stopwords off keeps the same word, so the two settings produce
	// different token streams for identical input.
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbFtEnableStopword, variable.Off))
	tokens, err = AnalyzeStandardV1(sctx, "The foo_bar, a 好")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "the", Position: 0},
		{Text: "foo_bar", Position: 1},
	}, tokens)

	// "cat" is not a stop word, so it survives either way.
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(variable.InnodbFtEnableStopword, variable.On))
	tokens, err = AnalyzeStandardV1(sctx, "The cat")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "cat", Position: 1},
	}, tokens)
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
		{Text: "a", Position: 0},
		{Text: "好", Position: 1},
		{Text: "xy", Position: 2},
	}, tokens)
}

func TestAnalyzeNgramV1(t *testing.T) {
	sctx := newFulltextTestContext(t)

	tokens, err := AnalyzeNgramV1(sctx, "Hi世界 foo-bar")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "hi", Position: 0},
		{Text: "i世", Position: 1},
		{Text: "世界", Position: 2},
		{Text: "fo", Position: 3},
		{Text: "oo", Position: 4},
		{Text: "ba", Position: 5},
		{Text: "ar", Position: 6},
	}, tokens)
}

func TestAnalyzeNgramV1ShortTokenAdvancesPositionBase(t *testing.T) {
	sctx := newFulltextTestContext(t)

	tokens, err := AnalyzeNgramV1(sctx, "abc x 好 y A_b 中z")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "ab", Position: 0},
		{Text: "bc", Position: 1},
		{Text: "a_", Position: 4},
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
		{Text: "abc", Position: 0},
		{Text: "bcd", Position: 1},
	}, tokens)
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

func TestAnalyzerConfigEqualComparesWhatTheParserReads(t *testing.T) {
	standard := AnalyzerConfig{
		ParserType:             model.FullTextParserTypeStandardV1,
		InnodbFtMinTokenSize:   3,
		InnodbFtMaxTokenSize:   84,
		InnodbFtEnableStopword: true,
	}
	// An index records no ngram size for STANDARD text, while a session always
	// carries the ngram_token_size default. That difference cannot change the
	// tokens, so it must not make the two look incompatible.
	withNgram := standard
	withNgram.NgramTokenSize = 2
	require.True(t, standard.Equal(withNgram))

	differentBound := standard
	differentBound.InnodbFtMinTokenSize = 5
	require.False(t, standard.Equal(differentBound))

	withoutStopwords := standard
	withoutStopwords.InnodbFtEnableStopword = false
	require.False(t, standard.Equal(withoutStopwords))

	ngram := AnalyzerConfig{ParserType: model.FullTextParserTypeNgramV1, NgramTokenSize: 2}
	require.False(t, standard.Equal(ngram))
	// The ngram analyzer neither length-filters nor removes stopwords, so
	// those settings cannot separate two ngram configurations.
	ngramWithBounds := ngram
	ngramWithBounds.InnodbFtMinTokenSize = 3
	ngramWithBounds.InnodbFtEnableStopword = true
	require.True(t, ngram.Equal(ngramWithBounds))

	differentNgram := ngram
	differentNgram.NgramTokenSize = 3
	require.False(t, ngram.Equal(differentNgram))
}
