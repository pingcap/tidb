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

import "strings"

type likePatternToken struct {
	literal  byte
	wildcard byte
}

// PathHierarchyLikeLiteral extracts the literal from a prefix LIKE pattern.
// For example, "/a/b/%" becomes "/a/b/".
func PathHierarchyLikeLiteral(pattern string, escape byte) (string, bool) {
	tokens, ok := tokenizeLikePattern(pattern, escape)
	if !ok || len(tokens) < 2 || tokens[len(tokens)-1].wildcard != '%' {
		return "", false
	}
	var literal strings.Builder
	for _, token := range tokens[:len(tokens)-1] {
		if token.wildcard != 0 {
			return "", false
		}
		literal.WriteByte(token.literal)
	}
	return literal.String(), literal.Len() > 0
}

// NgramLikeLiteral extracts the literal from a contains LIKE pattern.
// For example, "%hello%" becomes "hello".
func NgramLikeLiteral(pattern string, escape byte) (string, bool) {
	tokens, ok := tokenizeLikePattern(pattern, escape)
	if !ok || len(tokens) < 3 || tokens[0].wildcard != '%' || tokens[len(tokens)-1].wildcard != '%' {
		return "", false
	}
	var literal strings.Builder
	for _, token := range tokens[1 : len(tokens)-1] {
		if token.wildcard != 0 {
			return "", false
		}
		literal.WriteByte(token.literal)
	}
	return literal.String(), literal.Len() > 0
}

// LikeMandatoryLiterals returns the maximal literal runs that every string
// matching pattern must contain. Wildcards separate runs, while escaped
// wildcards remain part of a run.
func LikeMandatoryLiterals(pattern string, escape byte) ([]string, bool) {
	tokens, ok := tokenizeLikePattern(pattern, escape)
	if !ok {
		return nil, false
	}
	literals := make([]string, 0, 1)
	var literal strings.Builder
	flushLiteral := func() {
		if literal.Len() == 0 {
			return
		}
		literals = append(literals, literal.String())
		literal.Reset()
	}
	for _, token := range tokens {
		if token.wildcard != 0 {
			flushLiteral()
			continue
		}
		literal.WriteByte(token.literal)
	}
	flushLiteral()
	return literals, true
}

// PathHierarchyLikePrefilterLiteral extracts a complete hierarchy prefix from
// a prefix LIKE pattern whose literal ends in a partial path component. For
// example, with delimiter "/", "/a/b/prefix%" becomes "/a/b/". A root-only
// prefix is rejected because it would lower to an unselective match-all query.
func PathHierarchyLikePrefilterLiteral(pattern string, escape byte, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}
	literal, ok := PathHierarchyLikeLiteral(pattern, escape)
	if !ok || strings.HasSuffix(literal, delimiter) {
		return "", false
	}
	boundary := strings.LastIndex(literal, delimiter)
	if boundary < 0 {
		return "", false
	}
	prefix := literal[:boundary+len(delimiter)]
	queryLiteral := prefix
	for strings.HasSuffix(queryLiteral, delimiter) {
		queryLiteral = strings.TrimSuffix(queryLiteral, delimiter)
	}
	if queryLiteral == "" {
		return "", false
	}
	return prefix, true
}

func tokenizeLikePattern(pattern string, escape byte) ([]likePatternToken, bool) {
	tokens := make([]likePatternToken, 0, len(pattern))
	for i := 0; i < len(pattern); i++ {
		current := pattern[i]
		if current == escape {
			if i+1 >= len(pattern) {
				return nil, false
			}
			i++
			tokens = append(tokens, likePatternToken{literal: pattern[i]})
			continue
		}
		if current == '%' || current == '_' {
			tokens = append(tokens, likePatternToken{wildcard: current})
			continue
		}
		tokens = append(tokens, likePatternToken{literal: current})
	}
	return tokens, true
}
