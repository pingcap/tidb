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
	content string

	reservedKeywords   []string
	unreservedKeywords []string
	notKeywordTokens   []string
	tidbKeywords       []string
}

func (s *testConsistentSuite) SetUpSuite(c *C) {
	_, filename, _, _ := runtime.Caller(0)
	parserFilename := path.Join(path.Dir(filename), "parser.y")
	parserFile, err := os.Open(parserFilename)
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(parserFile)
	c.Assert(err, IsNil)
	s.content = string(data)

	reservedKeywordStartMarker := "\t/* The following tokens belong to ReservedKeyword. Notice: make sure these tokens are contained in ReservedKeyword. */"
	unreservedKeywordStartMarker := "\t/* The following tokens belong to UnReservedKeyword. Notice: make sure these tokens are contained in UnReservedKeyword. */"
	notKeywordTokenStartMarker := "\t/* The following tokens belong to NotKeywordToken. Notice: make sure these tokens are contained in NotKeywordToken. */"
	tidbKeywordStartMarker := "\t/* The following tokens belong to TiDBKeyword. Notice: make sure these tokens are contained in TiDBKeyword. */"
	identTokenEndMarker := "%token\t<item>"

	s.reservedKeywords = extractKeywords(s.content, reservedKeywordStartMarker, unreservedKeywordStartMarker)
	s.unreservedKeywords = extractKeywords(s.content, unreservedKeywordStartMarker, notKeywordTokenStartMarker)
	s.notKeywordTokens = extractKeywords(s.content, notKeywordTokenStartMarker, tidbKeywordStartMarker)
	s.tidbKeywords = extractKeywords(s.content, tidbKeywordStartMarker, identTokenEndMarker)
}

func (s *testConsistentSuite) TestKeywordConsistent(c *C) {
	for k, v := range aliases {
		c.Assert(k, Not(Equals), v)
		c.Assert(tokenMap[k], Equals, tokenMap[v])
	}
	keywordCount := len(s.reservedKeywords) + len(s.unreservedKeywords) + len(s.notKeywordTokens) + len(s.tidbKeywords)
	c.Assert(len(tokenMap)-len(aliases), Equals, keywordCount-len(windowFuncTokenMap))

	unreservedCollectionDef := extractKeywordsFromCollectionDef(s.content, "\nUnReservedKeyword:")
	c.Assert(s.unreservedKeywords, DeepEquals, unreservedCollectionDef)

	notKeywordTokensCollectionDef := extractKeywordsFromCollectionDef(s.content, "\nNotKeywordToken:")
	c.Assert(s.notKeywordTokens, DeepEquals, notKeywordTokensCollectionDef)

	tidbKeywordsCollectionDef := extractKeywordsFromCollectionDef(s.content, "\nTiDBKeyword:")
	c.Assert(s.tidbKeywords, DeepEquals, tidbKeywordsCollectionDef)
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
