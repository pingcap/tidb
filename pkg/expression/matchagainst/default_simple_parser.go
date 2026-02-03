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

package matchagainst

import "strings"

type SimpleGroup struct {
	Terms  []string
	Phrase []string
}

// ParseSimpleNaturalLanguageMode parses the AGAINST query in MySQL natural language mode into SimpleGroup.
//
// It extracts:
//   - Terms: "word" tokens outside quotes, consisting of unicode letters/digits/underscore.
//   - Phrase: double-quoted phrases, normalized into a space-separated list of word tokens.
//
// Any unterminated quote is treated as implicitly closed at the end.
func ParseSimpleNaturalLanguageMode(input string) SimpleGroup {
	var g SimpleGroup

	inQuote := false
	var phraseBuf []string

	var termBuf strings.Builder
	flushTerm := func() {
		if termBuf.Len() == 0 {
			return
		}
		term := termBuf.String()
		if inQuote {
			phraseBuf = append(phraseBuf, term)
		} else {
			g.Terms = append(g.Terms, term)
		}
		termBuf.Reset()
	}

	flushPhrase := func() {
		if len(phraseBuf) == 0 {
			return
		}
		g.Phrase = append(g.Phrase, strings.Join(phraseBuf, " "))
		phraseBuf = phraseBuf[:0]
	}

	for _, ch := range input {
		if ch == '"' {
			flushTerm()
			if inQuote {
				flushPhrase()
				inQuote = false
			} else {
				inQuote = true
			}
			continue
		}
		if isWordChar(ch) {
			termBuf.WriteRune(ch)
			continue
		}
		flushTerm()
	}
	flushTerm()
	if inQuote {
		flushPhrase()
	}
	return g
}
