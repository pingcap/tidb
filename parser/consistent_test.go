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
	"path"
	"runtime"
	"sort"
	"strings"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConsistentSuite{})

type testConsistentSuite struct {
}

func (s *testConsistentSuite) TestKeywordConsistent(c *C) {
	_, filename, _, _ := runtime.Caller(0)
	parserFilename := path.Join(path.Dir(filename), "parser.y")
	parserFile, err := os.Open(parserFilename)
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(parserFile)
	c.Assert(err, IsNil)
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
		c.Assert(k != v, IsTrue)
		c.Assert(tokenMap[k], Equals, tokenMap[v])
	}
	keywordCount := len(reservedKeywords) + len(unreservedKeywords) + len(notKeywordTokens) + len(tidbKeywords)
	c.Assert(len(tokenMap)-len(aliases), Equals, keywordCount-len(windowFuncTokenMap))

	unreservedCollectionDef := extractKeywordsFromCollectionDef(content, "\nUnReservedKeyword:")
	c.Assert(unreservedKeywords, DeepEquals, unreservedCollectionDef)

	notKeywordTokensCollectionDef := extractKeywordsFromCollectionDef(content, "\nNotKeywordToken:")
	c.Assert(notKeywordTokens, DeepEquals, notKeywordTokensCollectionDef)

	tidbKeywordsCollectionDef := extractKeywordsFromCollectionDef(content, "\nTiDBKeyword:")
	c.Assert(tidbKeywords, DeepEquals, tidbKeywordsCollectionDef)
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
