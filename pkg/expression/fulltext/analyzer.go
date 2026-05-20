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
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// Token is the analyzed fulltext token.
type Token struct {
	Text     string
	Position int
}

// PreserveUnderscoreTokenize tokenizes text with TiCI's PreserveUnderscore
// tokenizer semantics: Unicode alphanumeric characters and '_' form tokens;
// every other character is a delimiter.
func PreserveUnderscoreTokenize(text string) []Token {
	tokens := make([]Token, 0)
	pos := 0
	for i := 0; i < len(text); {
		ch, next := runeAtByte(text, i)
		if !isTokenChar(ch) {
			i = next
			continue
		}

		start := i
		j := next
		for j < len(text) {
			ch, next = runeAtByte(text, j)
			if !isTokenChar(ch) {
				break
			}
			j = next
		}
		tokens = append(tokens, Token{
			Text:     text[start:j],
			Position: pos,
		})
		pos++
		i = j
	}
	return tokens
}

// AnalyzeStandardV1 runs the STANDARD_V1 analyzer:
// PreserveUnderscore tokenizer, length filter, lower-case filter, and optional
// stopword filter.
func AnalyzeStandardV1(sctx sessionctx.Context, text string) ([]Token, error) {
	parserInfo, err := parserInfoFromSessionContext(sctx)
	if err != nil {
		return nil, err
	}
	return analyzeStandardV1(text, parserInfo), nil
}

func analyzeStandardV1(text string, parserInfo parserInfo) []Token {
	tokens := PreserveUnderscoreTokenize(text)
	tokens = lengthFilter(tokens, parserInfo.innodbFtMinTokenSize, parserInfo.innodbFtMaxTokenSize)
	tokens = lowerFilter(tokens)
	tokens = stopwordFilter(tokens, parserInfo)
	return tokens
}

// AnalyzeNgramV1 runs the NGRAM_V1 analyzer:
// PreserveUnderscore tokenizer, fixed-size ngram filter, and lower-case filter.
func AnalyzeNgramV1(sctx sessionctx.Context, text string) ([]Token, error) {
	parserInfo, err := parserInfoFromSessionContext(sctx)
	if err != nil {
		return nil, err
	}
	return analyzeNgramV1(text, parserInfo), nil
}

func analyzeNgramV1(text string, parserInfo parserInfo) []Token {
	tokens := PreserveUnderscoreTokenize(text)
	tokens = ngramFilter(tokens, parserInfo.ngramTokenSize, parserInfo.ngramTokenSize)
	tokens = lowerFilter(tokens)
	return tokens
}

type parserInfo struct {
	innodbFtMinTokenSize int
	innodbFtMaxTokenSize int
	ngramTokenSize       int
	stopwords            map[string]struct{}
}

func parserInfoFromSessionContext(sctx sessionctx.Context) (parserInfo, error) {
	if sctx == nil || sctx.GetSessionVars() == nil {
		return parserInfo{}, fmt.Errorf("missing session context for fulltext analyzer")
	}
	sessVars := sctx.GetSessionVars()

	enableStopword, err := getFulltextSysVar(sessVars, vardef.InnodbFtEnableStopword)
	if err != nil {
		return parserInfo{}, err
	}
	minTokenSize, err := getFulltextIntSysVar(sessVars, vardef.InnodbFtMinTokenSize)
	if err != nil {
		return parserInfo{}, err
	}
	maxTokenSize, err := getFulltextIntSysVar(sessVars, vardef.InnodbFtMaxTokenSize)
	if err != nil {
		return parserInfo{}, err
	}
	ngramTokenSize, err := getFulltextIntSysVar(sessVars, vardef.NgramTokenSize)
	if err != nil {
		return parserInfo{}, err
	}
	serverStopwordTable, err := getFulltextSysVar(sessVars, vardef.InnodbFtServerStopwordTable)
	if err != nil {
		return parserInfo{}, err
	}
	userStopwordTable, err := getFulltextSysVar(sessVars, vardef.InnodbFtUserStopwordTable)
	if err != nil {
		return parserInfo{}, err
	}
	stopwords := stopwordSetFromSysVars(enableStopword, serverStopwordTable, userStopwordTable)

	return parserInfo{
		innodbFtMinTokenSize: minTokenSize,
		innodbFtMaxTokenSize: maxTokenSize,
		ngramTokenSize:       ngramTokenSize,
		stopwords:            stopwords,
	}, nil
}

func stopwordSetFromSysVars(enableStopword, serverStopwordTable, userStopwordTable string) map[string]struct{} {
	if !variable.TiDBOptOn(enableStopword) {
		return nil
	}
	if strings.TrimSpace(serverStopwordTable) != "" || strings.TrimSpace(userStopwordTable) != "" {
		return map[string]struct{}{}
	}
	return builtinInnoDBEnglishStopwords
}

func getFulltextSysVar(sessVars *variable.SessionVars, name string) (string, error) {
	val, err := sessVars.GetSessionOrGlobalSystemVar(context.Background(), name)
	if err != nil {
		return "", fmt.Errorf("get %s for fulltext analyzer: %w", name, err)
	}
	return val, nil
}

func getFulltextIntSysVar(sessVars *variable.SessionVars, name string) (int, error) {
	val, err := getFulltextSysVar(sessVars, name)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("parse %s for fulltext analyzer: %w", name, err)
	}
	return n, nil
}

func runeAtByte(text string, offset int) (rune, int) {
	ch, size := utf8.DecodeRuneInString(text[offset:])
	if size == 0 {
		return 0, len(text)
	}
	return ch, offset + size
}

func isTokenChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsNumber(ch) || ch == '_'
}

func lengthFilter(tokens []Token, minLen, maxLen int) []Token {
	if minLen > maxLen {
		return nil
	}

	out := make([]Token, 0, len(tokens))
	for _, token := range tokens {
		n := charLen(token.Text)
		if minLen <= n && n <= maxLen {
			out = append(out, token)
		}
	}
	return out
}

func charLen(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

func lowerFilter(tokens []Token) []Token {
	for i := range tokens {
		tokens[i].Text = strings.ToLower(tokens[i].Text)
	}
	return tokens
}

func stopwordFilter(tokens []Token, parserInfo parserInfo) []Token {
	if parserInfo.stopwords == nil {
		return tokens
	}

	out := make([]Token, 0, len(tokens))
	for _, token := range tokens {
		if _, ok := parserInfo.stopwords[token.Text]; !ok {
			out = append(out, token)
		}
	}
	return out
}

func stopwordSet(words ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(words))
	for _, word := range words {
		set[word] = struct{}{}
	}
	return set
}

func ngramFilter(tokens []Token, minGram, maxGram int) []Token {
	if minGram <= 0 || maxGram < minGram {
		return nil
	}

	out := make([]Token, 0)
	nextPositionBase := 0
	for _, token := range tokens {
		basePosition := max(token.Position, nextPositionBase)
		spans := utf8CharSpans(token.Text)
		charCount := len(spans)
		if charCount < minGram {
			nextPositionBase = max(nextPositionBase, token.Position+1)
			continue
		}

		maxLimit := min(maxGram, charCount)
		for startIdx := range charCount {
			for gramLen := minGram; gramLen <= maxLimit; gramLen++ {
				endIdx := startIdx + gramLen
				if endIdx > charCount {
					break
				}

				startByte := spans[startIdx].byteStart
				endByte := spans[endIdx-1].byteEnd
				out = append(out, Token{
					Text:     token.Text[startByte:endByte],
					Position: basePosition + startIdx,
				})
			}
		}
		nextPositionBase = basePosition + charCount - minGram + 1
	}
	return out
}

type charSpan struct {
	byteStart int
	byteEnd   int
}

func utf8CharSpans(text string) []charSpan {
	spans := make([]charSpan, 0, len(text))
	for start, ch := range text {
		size := utf8.RuneLen(ch)
		if size < 0 {
			size = 1
		}
		spans = append(spans, charSpan{
			byteStart: start,
			byteEnd:   start + size,
		})
	}
	return spans
}

// builtinInnoDBEnglishStopwords is used by the STANDARD analyzer when
// innodb_ft_enable_stopword is ON and no custom stopword table is configured.
var builtinInnoDBEnglishStopwords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {}, "be": {}, "but": {},
	"by": {}, "for": {}, "if": {}, "in": {}, "into": {}, "is": {}, "it": {}, "no": {},
	"not": {}, "of": {}, "on": {}, "or": {}, "such": {}, "that": {}, "the": {},
	"their": {}, "then": {}, "there": {}, "these": {}, "they": {}, "this": {}, "to": {},
	"was": {}, "will": {}, "with": {},
}
