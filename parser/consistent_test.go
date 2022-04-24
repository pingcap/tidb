// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"

	requires "github.com/stretchr/testify/require"
)

func TestKeywordConsistent(t *testing.T) {
	parserFilename := "parser.y"
	parserFile, err := os.Open(parserFilename)
	requires.NoError(t, err)
	data, err := ioutil.ReadAll(parserFile)
	requires.NoError(t, err)
	content := string(data)

	reservedKeywordStartMarker := "\t/* The following tokens belong to ReservedKeyword. Notice: make sure these tokens are contained in ReservedKeyword. */"
	unreservedKeywordStartMarker := "\t/* The following tokens belong to UnReservedKeyword. Notice: make sure these tokens are contained in UnReservedKeyword. */"
	notKeywordTokenStartMarker := "\t/* The following tokens belong to NotKeywordToken. Notice: make sure these tokens are contained in NotKeywordToken. */"
	tidbKeywordStartMarker := "\t/* The following tokens belong to TiDBKeyword. Notice: make sure these tokens are contained in TiDBKeyword. */"
	identTokenEndMarker := "%token\t<item>"

	reservedKeywords := extractKeywords(content, reservedKeywordStartMarker, unreservedKeywordStartMarker)
	unreservedKeywords := extractKeywords(content, unreservedKeywordStartMarker, notKeywordTokenStartMarker)
	notKeywordTokens := extractKeywords(content, notKeywordTokenStartMarker, tidbKeywordStartMarker)
	tidbKeywords := extractKeywords(content, tidbKeywordStartMarker, identTokenEndMarker)

	for k, v := range aliases {
		requires.NotEqual(t, k, v)
		requires.Equal(t, tokenMap[v], tokenMap[k])
	}
	keywordCount := len(reservedKeywords) + len(unreservedKeywords) + len(notKeywordTokens) + len(tidbKeywords)
	requires.Equal(t, keywordCount-len(windowFuncTokenMap), len(tokenMap)-len(aliases))

	unreservedCollectionDef := extractKeywordsFromCollectionDef(content, "\nUnReservedKeyword:")
	requires.Equal(t, unreservedCollectionDef, unreservedKeywords)

	notKeywordTokensCollectionDef := extractKeywordsFromCollectionDef(content, "\nNotKeywordToken:")
	requires.Equal(t, notKeywordTokensCollectionDef, notKeywordTokens)

	tidbKeywordsCollectionDef := extractKeywordsFromCollectionDef(content, "\nTiDBKeyword:")
	requires.Equal(t, tidbKeywordsCollectionDef, tidbKeywords)
}

func extractMiddle(str, startMarker, endMarker string) string {
	startIdx := strings.Index(str, startMarker)
	if startIdx == -1 {
		return ""
	}
	str = str[startIdx+len(startMarker):]
	endIdx := strings.Index(str, endMarker)
	if endIdx == -1 {
		return ""
	}
	return str[:endIdx]
}

func extractQuotedWords(strs []string) []string {
	var words []string
	for _, str := range strs {
		word := extractMiddle(str, "\"", "\"")
		if word == "" {
			continue
		}
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func extractKeywords(content, startMarker, endMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, endMarker)
	lines := strings.Split(keywordSection, "\n")
	return extractQuotedWords(lines)
}

func extractKeywordsFromCollectionDef(content, startMarker string) []string {
	keywordSection := extractMiddle(content, startMarker, "\n\n")
	words := strings.Split(keywordSection, "|")
	return extractQuotedWords(words)
}
