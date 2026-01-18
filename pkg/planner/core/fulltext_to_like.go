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
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
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
	phraseIsRequired := false
	phraseIsExcluded := false
	i := 0

	for i < len(text) {
		ch := text[i]

		switch ch {
		case '"':
			if inQuote {
				// End of phrase
				// NOTE: Phrase matching in MySQL full-text search finds the exact phrase as a sequence
				// of words (word boundaries are enforced). Using LIKE %phrase%, we cannot perfectly
				// enforce word boundaries without REGEXP. For example, "quick brown" would match
				// "aquick brownie" which MySQL full-text search would not match. This is an acceptable
				// limitation for a fallback implementation.
				phrase := current.String()
				if phrase != "" {
					terms = append(terms, searchTerm{
						word:       phrase,
						isRequired: phraseIsRequired,
						isExcluded: phraseIsExcluded,
						isPhrase:   true,
					})
				}
				current.Reset()
				inQuote = false
				phraseIsRequired = false
				phraseIsExcluded = false
			} else {
				// Check for leading operator before the quote (e.g., +"phrase" or -"phrase")
				if current.Len() > 0 {
					prefix := current.String()
					if len(prefix) > 0 {
						if prefix[0] == '+' {
							phraseIsRequired = true
						} else if prefix[0] == '-' {
							phraseIsExcluded = true
						}
					}
					current.Reset()
				}
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

// hasFulltextIndex checks if a fulltext index exists for the given columns
func hasFulltextIndex(is infoschema.InfoSchema, columnNames []*ast.ColumnName) (bool, error) {
	if len(columnNames) == 0 {
		return false, nil
	}

	// All columns in a MATCH clause must be from the same table
	// Get the schema and table from the first column
	schema := columnNames[0].Schema
	tableName := columnNames[0].Table

	// If schema is not specified, we cannot determine the table
	// In this case, we'll need to check later during execution
	if tableName.L == "" {
		return false, nil
	}

	// Get the table from info schema
	tbl, err := is.TableByName(nil, schema, tableName)
	if err != nil {
		// Table not found, cannot check for fulltext index
		return false, nil
	}

	tblInfo := tbl.Meta()
	if tblInfo == nil {
		return false, nil
	}

	// Extract column names from the MATCH clause
	matchColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		matchColumns[i] = col.Name.L // Use lowercase for case-insensitive comparison
	}

	// Check each index to see if it's a fulltext index covering the exact columns
	for _, idx := range tblInfo.Indices {
		// Check if this is a fulltext index
		if idx.Tp != ast.IndexTypeFulltext {
			continue
		}

		// Check if the index is in a usable state
		if idx.State != model.StatePublic {
			continue
		}

		// Check if the index covers the exact set of columns
		if len(idx.Columns) != len(matchColumns) {
			continue
		}

		// Extract index column names
		idxColumns := make([]string, len(idx.Columns))
		for i, col := range idx.Columns {
			idxColumns[i] = col.Name.L
		}

		// Check if the columns match (order doesn't matter for MATCH...AGAINST)
		slices.Sort(matchColumns)
		slices.Sort(idxColumns)
		if slices.Equal(matchColumns, idxColumns) {
			return true, nil
		}
	}

	return false, nil
}

// convertMatchAgainstToLike converts a MATCH...AGAINST expression to LIKE predicates
//
// This is a fallback implementation since TiDB does not natively support full-text search.
// It provides basic text matching capabilities but has the following semantic differences
// from MySQL's full-text search:
//
// 1. No relevance scoring - returns 1 for match, 0 for no match (MySQL returns a relevance score)
// 2. No stop word filtering - searches for all words regardless of length or commonness
// 3. No word length limits - MySQL ignores words shorter than ft_min_word_len (default 4)
// 4. No word boundaries - LIKE %term% matches substrings anywhere, not just complete words
//   - Simple terms: "cat" matches "concatenate", "category", "application"
//     (MySQL FTS only matches "cat" as a standalone word)
//   - Prefix wildcard: "Optim*" matches "reOptimizing", "Optimizing"
//     (MySQL FTS only matches words starting with "Optim" like "Optimizing", not "reOptimizing")
//   - Phrase matching: "quick brown" matches "aquick brownie"
//     (MySQL FTS only matches the exact phrase with word boundaries)
//   This limitation exists because LIKE cannot enforce word boundaries without REGEXP
//
// 5. Case sensitivity - follows column collation (MySQL full-text search is case-insensitive)
// 6. Performance - LIKE predicates cannot use full-text indexes (much slower on large datasets)
//
// Supported Boolean mode operators: + (required), - (excluded), * (prefix wildcard), "..." (phrase)
// Unsupported operators: ~ (negation with ranking), > < (relevance modifiers), () (grouping)
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

		// Build predicates with correct Boolean logic for multiple columns
		// In MySQL, MATCH(col1, col2) AGAINST('+word1 +word2') means:
		// - word1 must appear in (col1 OR col2)
		// - word2 must appear in (col1 OR col2)
		var allPredicates []expression.Expression

		// For each required term: (col1 LIKE %term% OR col2 LIKE %term%)
		for _, term := range required {
			var termColumnPreds []expression.Expression
			for _, column := range columns {
				pred, err := er.buildLikePredicate(column, term.word, false, term.isPrefixMatch)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			// At least one column must match this required term
			if len(termColumnPreds) > 0 {
				allPredicates = append(allPredicates, expression.ComposeDNFCondition(er.sctx, termColumnPreds...))
			}
		}

		// For each excluded term: NOT(col1 LIKE %term% OR col2 LIKE %term%)
		for _, term := range excluded {
			var termColumnPreds []expression.Expression
			for _, column := range columns {
				pred, err := er.buildLikePredicate(column, term.word, false, term.isPrefixMatch)
				if err != nil {
					return nil, err
				}
				termColumnPreds = append(termColumnPreds, pred)
			}
			// None of the columns should match this excluded term
			if len(termColumnPreds) > 0 {
				notPred, err := er.newFunction(ast.UnaryNot, types.NewFieldType(mysql.TypeTiny),
					expression.ComposeDNFCondition(er.sctx, termColumnPreds...))
				if err != nil {
					return nil, err
				}
				allPredicates = append(allPredicates, notPred)
			}
		}

		// For optional terms: OR across all term-column combinations
		if len(optional) > 0 {
			var allOptionalPreds []expression.Expression
			for _, term := range optional {
				for _, column := range columns {
					pred, err := er.buildLikePredicate(column, term.word, false, term.isPrefixMatch)
					if err != nil {
						return nil, err
					}
					allOptionalPreds = append(allOptionalPreds, pred)
				}
			}
			if len(allOptionalPreds) > 0 {
				allPredicates = append(allPredicates, expression.ComposeDNFCondition(er.sctx, allOptionalPreds...))
			}
		}

		// AND all predicates together
		if len(allPredicates) == 0 {
			return &expression.Constant{
				Value:   types.NewIntDatum(0),
				RetType: types.NewFieldType(mysql.TypeTiny),
			}, nil
		}

		return expression.ComposeCNFCondition(er.sctx, allPredicates...), nil
	}

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
			pred, err := er.buildLikePredicate(column, word, false, false)
			if err != nil {
				return nil, err
			}
			wordPredicates = append(wordPredicates, pred)
		}
		if len(wordPredicates) > 0 {
			columnPredicates = append(columnPredicates, expression.ComposeDNFCondition(er.sctx, wordPredicates...))
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
	// Count special characters to pre-allocate the exact buffer size needed
	escapeCount := 0
	for i := range len(term) {
		ch := term[i]
		if ch == '\\' || ch == '%' || ch == '_' {
			escapeCount++
		}
	}

	// Allocate exact size: original length + number of escape characters
	var result strings.Builder
	result.Grow(len(term) + escapeCount)
	for i := range len(term) {
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
	_ bool,
) (expression.Expression, error) {
	// Escape special LIKE characters in the search term
	escapedTerm := escapeLikePattern(term)

	// Build the pattern
	// NOTE: Prefix matching (word*) in MySQL full-text search matches words that START with
	// the prefix, but the word can appear anywhere in the text. For example, "Optim*" should
	// match "Optimizing MySQL" but NOT "reOptimizing". Using LIKE without REGEXP, we cannot
	// perfectly enforce word-start boundaries. We use %term% which may produce false positives
	// (matching mid-word like "reOptimizing"), but avoids false negatives. This is an acceptable
	// limitation for a fallback implementation.
	// Both prefix and general matches use %term% to find the term anywhere in text
	pattern := "%" + escapedTerm + "%"

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
