// Copyright 2025 PingCAP, Inc.
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

package core

import (
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// searchTerm represents a single term in a Boolean fulltext search query
type searchTerm struct {
	word          string
	isRequired    bool // Has '+' prefix
	isExcluded    bool // Has '-' prefix
	isPrefixMatch bool // Has '*' suffix
	isPhrase      bool // Wrapped in quotes
}

// parseBooleanSearchString parses a Boolean mode search string into individual terms
func parseBooleanSearchString(text string) []searchTerm {
	var terms []searchTerm
	var current strings.Builder
	inQuote := false
	i := 0

	for i < len(text) {
		ch := text[i]

		switch ch {
		case '"':
			if inQuote {
				// End of phrase
				phrase := current.String()
				if phrase != "" {
					terms = append(terms, searchTerm{
						word:     phrase,
						isPhrase: true,
					})
				}
				current.Reset()
				inQuote = false
			} else {
				// Start of phrase
				inQuote = true
			}
			i++
		case ' ', '\t', '\n', '\r':
			if inQuote {
				current.WriteByte(ch)
			} else if current.Len() > 0 {
				// End of word
				word := current.String()
				terms = append(terms, parseSearchTerm(word))
				current.Reset()
			}
			i++
		default:
			current.WriteByte(ch)
			i++
		}
	}

	// Handle remaining content
	if current.Len() > 0 {
		if inQuote {
			// Unclosed quote, treat as phrase
			terms = append(terms, searchTerm{
				word:     current.String(),
				isPhrase: true,
			})
		} else {
			word := current.String()
			terms = append(terms, parseSearchTerm(word))
		}
	}

	return terms
}

// parseSearchTerm parses a single search term (not in quotes) and extracts operators
func parseSearchTerm(word string) searchTerm {
	if word == "" {
		return searchTerm{}
	}

	term := searchTerm{word: word}

	// Check for leading operators
	if word[0] == '+' {
		term.isRequired = true
		word = word[1:]
	} else if word[0] == '-' {
		term.isExcluded = true
		word = word[1:]
	}

	// Check for trailing wildcard
	if len(word) > 0 && word[len(word)-1] == '*' {
		term.isPrefixMatch = true
		word = word[:len(word)-1]
	}

	term.word = word
	return term
}

// convertMatchAgainstToLike converts a MATCH...AGAINST expression to LIKE predicates
func (er *expressionRewriter) convertMatchAgainstToLike(
	columns []expression.Expression,
	searchText string,
	modifier ast.FulltextSearchModifier,
) (expression.Expression, error) {
	if len(columns) == 0 {
		return nil, expression.ErrNotSupportedYet.GenWithStackByArgs("MATCH...AGAINST with no columns")
	}

	if searchText == "" {
		// Empty search string matches nothing
		return &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}

	var columnPredicates []expression.Expression

	if modifier.IsBooleanMode() {
		// Parse Boolean mode search string
		terms := parseBooleanSearchString(searchText)
		if len(terms) == 0 {
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		// Group terms by type
		var required, excluded, optional []searchTerm
		for _, term := range terms {
			if term.word == "" {
				continue
			}
			if term.isRequired {
				required = append(required, term)
			} else if term.isExcluded {
				excluded = append(excluded, term)
			} else {
				optional = append(optional, term)
			}
		}

		// Build predicates for each column
		for _, column := range columns {
			var predicates []expression.Expression

			// AND all required terms
			for _, term := range required {
				pred, err := er.buildLikePredicate(column, term.word, false, term.isPrefixMatch, term.isPhrase)
				if err != nil {
					return nil, err
				}
				predicates = append(predicates, pred)
			}

			// AND NOT all excluded terms
			for _, term := range excluded {
				pred, err := er.buildLikePredicate(column, term.word, true, term.isPrefixMatch, term.isPhrase)
				if err != nil {
					return nil, err
				}
				predicates = append(predicates, pred)
			}

			// OR all optional terms (if any)
			if len(optional) > 0 {
				var optionalPreds []expression.Expression
				for _, term := range optional {
					pred, err := er.buildLikePredicate(column, term.word, false, term.isPrefixMatch, term.isPhrase)
					if err != nil {
						return nil, err
					}
					optionalPreds = append(optionalPreds, pred)
				}
				if len(optionalPreds) > 0 {
					predicates = append(predicates, expression.ComposeDNFCondition(er.sctx, optionalPreds...))
				}
			}

			// If we have any predicates for this column, combine them with AND
			if len(predicates) > 0 {
				columnPredicates = append(columnPredicates, expression.ComposeCNFCondition(er.sctx, predicates...))
			}
		}
	} else {
		// Natural Language Mode: split into words and OR them together
		words := strings.Fields(searchText)
		if len(words) == 0 {
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		for _, column := range columns {
			var wordPredicates []expression.Expression
			for _, word := range words {
				pred, err := er.buildLikePredicate(column, word, false, false, false)
				if err != nil {
					return nil, err
				}
				wordPredicates = append(wordPredicates, pred)
			}
			if len(wordPredicates) > 0 {
				columnPredicates = append(columnPredicates, expression.ComposeDNFCondition(er.sctx, wordPredicates...))
			}
		}
	}

	// OR across all columns
	if len(columnPredicates) == 0 {
		return &expression.Constant{
			Value:   types.NewIntDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}, nil
	}

	return expression.ComposeDNFCondition(er.sctx, columnPredicates...), nil
}

// escapeLikePattern escapes special LIKE characters (%, _, \) in the search term
// so they are treated as literal characters rather than wildcards
func escapeLikePattern(term string) string {
	var result strings.Builder
	result.Grow(len(term))
	for i := 0; i < len(term); i++ {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			result.WriteByte('\\')
		}
		result.WriteByte(ch)
	}
	return result.String()
}

// buildLikePredicate builds a single LIKE predicate for a column and search term
func (er *expressionRewriter) buildLikePredicate(
	column expression.Expression,
	term string,
	isNegated bool,
	isPrefixMatch bool,
	isPhrase bool,
) (expression.Expression, error) {
	// Escape special LIKE characters in the search term
	escapedTerm := escapeLikePattern(term)

	// Build the pattern
	var pattern string
	if isPhrase {
		// Exact phrase: %term%
		pattern = "%" + escapedTerm + "%"
	} else if isPrefixMatch {
		// Prefix match: term%
		pattern = escapedTerm + "%"
	} else {
		// General match: %term%
		pattern = "%" + escapedTerm + "%"
	}

	// Create constant for pattern
	patternConst := &expression.Constant{
		Value:   types.NewStringDatum(pattern),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}

	// Create escape constant (backslash = 92)
	escapeConst := &expression.Constant{
		Value:   types.NewIntDatum(92),
		RetType: types.NewFieldType(mysql.TypeTiny),
	}

	// Build LIKE function
	likeFunc, err := er.newFunction(ast.Like, types.NewFieldType(mysql.TypeTiny), column, patternConst, escapeConst)
	if err != nil {
		return nil, err
	}

	// Apply NOT if needed
	if isNegated {
		notFunc, err := er.newFunction(ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), likeFunc)
		if err != nil {
			return nil, err
		}
		return notFunc, nil
	}

	return likeFunc, nil
}
