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

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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

	tokens, err := AnalyzeStandardV1(sctx, "The foo_bar, a 好")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "the", Position: 0},
		{Text: "foo_bar", Position: 1},
	}, tokens)

	require.NoError(t, sctx.GetSessionVars().SetSystemVar(vardef.InnodbFtEnableStopword, vardef.Off))
	tokens, err = AnalyzeStandardV1(sctx, "The cat")
	require.NoError(t, err)
	require.Equal(t, []Token{
		{Text: "the", Position: 0},
		{Text: "cat", Position: 1},
	}, tokens)
}

func TestAnalyzeStandardV1ReadsTokenSizesFromSessionContext(t *testing.T) {
	sctx := newFulltextTestContext(t)
	setGlobalSysVar(t, sctx, vardef.InnodbFtMinTokenSize, "1")
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(vardef.InnodbFtEnableStopword, vardef.Off))

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
	setGlobalSysVar(t, sctx, vardef.NgramTokenSize, "3")

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
