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
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// Token is the analyzed fulltext token.
type Token struct {
	Text string
	// Position is the token-stream ordinal, not a byte or rune offset.
	Position int
}

// AnalyzerConfig is the fulltext analyzer configuration used for local
// MATCH ... AGAINST evaluation.
type AnalyzerConfig struct {
	ParserType             model.FullTextParserType
	InnodbFtMinTokenSize   int
	InnodbFtMaxTokenSize   int
	InnodbFtEnableStopword bool
	Stopwords              []string
	NgramTokenSize         int
}

// Equal reports whether two analyzer configurations produce the same token
// stream. Stopword order does not affect analysis; DDL stores it sorted, so an
// order-sensitive comparison also catches malformed or non-canonical metadata.
func (c AnalyzerConfig) Equal(other AnalyzerConfig) bool {
	return c.ParserType == other.ParserType &&
		c.InnodbFtMinTokenSize == other.InnodbFtMinTokenSize &&
		c.InnodbFtMaxTokenSize == other.InnodbFtMaxTokenSize &&
		c.InnodbFtEnableStopword == other.InnodbFtEnableStopword &&
		c.NgramTokenSize == other.NgramTokenSize &&
		slices.Equal(c.Stopwords, other.Stopwords)
}

// Analyzer analyzes text into fulltext tokens.
type Analyzer interface {
	Analyze(text string) ([]Token, error)
}

type analyzerFunc func(text string) ([]Token, error)

func (f analyzerFunc) Analyze(text string) ([]Token, error) {
	return f(text)
}

// GetAnalyzer returns the analyzer for the selected fulltext parser type.
func GetAnalyzer(config AnalyzerConfig) (Analyzer, error) {
	parserInfo := parserInfoFromConfig(config)
	switch config.ParserType {
	case model.FullTextParserTypeStandardV1:
		return analyzerFunc(func(text string) ([]Token, error) {
			return analyzeStandardV1(text, parserInfo), nil
		}), nil
	case model.FullTextParserTypeNgramV1:
		return analyzerFunc(func(text string) ([]Token, error) {
			return analyzeNgramV1(text, parserInfo), nil
		}), nil
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type: %s", config.ParserType)
	}
}

// AnalyzerConfigFromSessionContext builds an AnalyzerConfig from the current
// session/global sysvars. Local index-backed MATCH should prefer index-bound
// config once it is persisted in table metadata.
func AnalyzerConfigFromSessionContext(sctx sessionctx.Context, parserType model.FullTextParserType) (AnalyzerConfig, error) {
	if sctx == nil || sctx.GetSessionVars() == nil {
		return AnalyzerConfig{}, fmt.Errorf("missing session context for fulltext analyzer")
	}
	return AnalyzerConfigFromSessionVars(sctx.GetSessionVars(), parserType)
}

// AnalyzerConfigFromSessionVars builds an AnalyzerConfig from session/global
// sysvars.
func AnalyzerConfigFromSessionVars(sessVars *variable.SessionVars, parserType model.FullTextParserType) (AnalyzerConfig, error) {
	if sessVars == nil {
		return AnalyzerConfig{}, fmt.Errorf("missing session vars for fulltext analyzer")
	}
	enableStopword, err := getFulltextSysVar(sessVars, vardef.InnodbFtEnableStopword)
	if err != nil {
		return AnalyzerConfig{}, err
	}
	minTokenSize, err := getFulltextIntSysVar(sessVars, vardef.InnodbFtMinTokenSize)
	if err != nil {
		return AnalyzerConfig{}, err
	}
	maxTokenSize, err := getFulltextIntSysVar(sessVars, vardef.InnodbFtMaxTokenSize)
	if err != nil {
		return AnalyzerConfig{}, err
	}
	ngramTokenSize, err := getFulltextIntSysVar(sessVars, vardef.NgramTokenSize)
	if err != nil {
		return AnalyzerConfig{}, err
	}

	return AnalyzerConfig{
		ParserType:             parserType,
		InnodbFtMinTokenSize:   minTokenSize,
		InnodbFtMaxTokenSize:   maxTokenSize,
		InnodbFtEnableStopword: variable.TiDBOptOn(enableStopword),
		NgramTokenSize:         ngramTokenSize,
	}, nil
}

// AnalyzerConfigFromFullTextIndexInfo restores the analyzer snapshot captured
// when TiCI created the index. Older metadata without a snapshot is rejected:
// mutable query-time sysvars cannot prove semantic equivalence with that index.
func AnalyzerConfigFromFullTextIndexInfo(indexInfo *model.FullTextIndexInfo) (AnalyzerConfig, error) {
	if indexInfo == nil {
		return AnalyzerConfig{}, fmt.Errorf("missing fulltext index info")
	}
	if indexInfo.ParserConfig == nil {
		return AnalyzerConfig{}, fmt.Errorf("fulltext index is missing its analyzer configuration snapshot")
	}
	params := indexInfo.ParserConfig.ParserParams
	getParam := func(name string) (string, error) {
		value, ok := params[name]
		if !ok {
			return "", fmt.Errorf("fulltext index analyzer configuration is missing %s", name)
		}
		return value, nil
	}
	getIntParam := func(name string) (int, error) {
		value, err := getParam(name)
		if err != nil {
			return 0, err
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("parse fulltext index analyzer parameter %s: %w", name, err)
		}
		return parsed, nil
	}

	enableStopword, err := getParam(vardef.InnodbFtEnableStopword)
	if err != nil {
		return AnalyzerConfig{}, err
	}
	config := AnalyzerConfig{
		ParserType:             indexInfo.ParserType,
		InnodbFtEnableStopword: variable.TiDBOptOn(enableStopword),
		Stopwords:              slices.Clone(indexInfo.ParserConfig.StopWords),
	}
	switch indexInfo.ParserType {
	case model.FullTextParserTypeStandardV1:
		config.InnodbFtMinTokenSize, err = getIntParam(vardef.InnodbFtMinTokenSize)
		if err != nil {
			return AnalyzerConfig{}, err
		}
		config.InnodbFtMaxTokenSize, err = getIntParam(vardef.InnodbFtMaxTokenSize)
		if err != nil {
			return AnalyzerConfig{}, err
		}
	case model.FullTextParserTypeNgramV1:
		config.NgramTokenSize, err = getIntParam(vardef.NgramTokenSize)
		if err != nil {
			return AnalyzerConfig{}, err
		}
	default:
		return AnalyzerConfig{}, fmt.Errorf("unsupported fulltext parser type for local evaluation: %s", indexInfo.ParserType)
	}
	return config, nil
}

// PreserveUnderscoreTokenize tokenizes text with TiCI's PreserveUnderscore
// tokenizer semantics: Unicode alphanumeric characters and '_' form tokens;
// every other character is a delimiter.
func PreserveUnderscoreTokenize(text string) []Token {
	tokens := make([]Token, 0)
	tokenPos := 0
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
			Position: tokenPos,
		})
		tokenPos++
		i = j
	}
	return tokens
}

// AnalyzeStandardV1 runs the STANDARD_V1 analyzer:
// PreserveUnderscore tokenizer, length filter, lower-case filter, and optional
// stopword filter.
func AnalyzeStandardV1(sctx sessionctx.Context, text string) ([]Token, error) {
	config, err := AnalyzerConfigFromSessionContext(sctx, model.FullTextParserTypeStandardV1)
	if err != nil {
		return nil, err
	}
	analyzer, err := GetAnalyzer(config)
	if err != nil {
		return nil, err
	}
	return analyzer.Analyze(text)
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
	config, err := AnalyzerConfigFromSessionContext(sctx, model.FullTextParserTypeNgramV1)
	if err != nil {
		return nil, err
	}
	analyzer, err := GetAnalyzer(config)
	if err != nil {
		return nil, err
	}
	return analyzer.Analyze(text)
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
	config, err := AnalyzerConfigFromSessionContext(sctx, model.FullTextParserTypeStandardV1)
	if err != nil {
		return parserInfo{}, err
	}
	return parserInfoFromConfig(config), nil
}

func parserInfoFromConfig(config AnalyzerConfig) parserInfo {
	return parserInfo{
		innodbFtMinTokenSize: config.InnodbFtMinTokenSize,
		innodbFtMaxTokenSize: config.InnodbFtMaxTokenSize,
		ngramTokenSize:       config.NgramTokenSize,
		stopwords:            stopwordSetFromConfig(config),
	}
}

func stopwordSetFromConfig(config AnalyzerConfig) map[string]struct{} {
	if !config.InnodbFtEnableStopword {
		return nil
	}
	// TiDB does not resolve InnoDB stopword table contents on this path yet.
	if len(config.Stopwords) == 0 {
		return map[string]struct{}{}
	}
	set := make(map[string]struct{}, len(config.Stopwords))
	for _, word := range config.Stopwords {
		set[strings.ToLower(word)] = struct{}{}
	}
	return set
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

	out := tokens[:0]
	for _, token := range tokens {
		n := charLen(token.Text)
		if minLen <= n && n <= maxLen {
			out = append(out, token)
		}
	}
	clear(tokens[len(out):])
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

	out := tokens[:0]
	for _, token := range tokens {
		if _, ok := parserInfo.stopwords[token.Text]; !ok {
			out = append(out, token)
		}
	}
	clear(tokens[len(out):])
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
	for start := 0; start < len(text); {
		_, size := utf8.DecodeRuneInString(text[start:])
		end := start + size
		spans = append(spans, charSpan{
			byteStart: start,
			byteEnd:   end,
		})
		start = end
	}
	return spans
}
