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
	"sort"

	"github.com/pingcap/tidb/pkg/util/collate"
)

// ColumnInput is one MATCH column value for local fulltext evaluation.
type ColumnInput struct {
	Text   string
	IsNull bool
}

// ColumnDocument is the analyzed token stream for one MATCH column.
type ColumnDocument struct {
	ColumnOrdinal int
	Tokens        []Token
	Positions     map[string][]int
}

// Document is the analyzed row document used by local no-score fulltext
// matching.
type Document struct {
	Columns   []ColumnDocument
	TokenSet  map[string]struct{}
	TokenFreq map[string]int
}

// BuildDocument analyzes MATCH column values with the selected analyzer.
// NULL columns and empty strings contribute no tokens.
func BuildDocument(columns []ColumnInput, analyzer Analyzer) (*Document, error) {
	doc := &Document{
		Columns:   make([]ColumnDocument, 0, len(columns)),
		TokenSet:  make(map[string]struct{}),
		TokenFreq: make(map[string]int),
	}
	for i, column := range columns {
		colDoc := ColumnDocument{
			ColumnOrdinal: i,
			Positions:     make(map[string][]int),
		}
		if !column.IsNull && column.Text != "" {
			tokens, err := analyzer.Analyze(column.Text)
			if err != nil {
				return nil, err
			}
			colDoc.Tokens = tokens
			for _, token := range tokens {
				doc.TokenSet[token.Text] = struct{}{}
				doc.TokenFreq[token.Text]++
				colDoc.Positions[token.Text] = append(colDoc.Positions[token.Text], token.Position)
			}
		}
		doc.Columns = append(doc.Columns, colDoc)
	}
	return doc, nil
}

func (doc *Document) hasToken(token string, collator collate.Collator) bool {
	if doc == nil {
		return false
	}
	if collator == nil {
		_, ok := doc.TokenSet[token]
		return ok
	}
	for candidate := range doc.TokenSet {
		if collator.Compare(candidate, token) == 0 {
			return true
		}
	}
	return false
}

func (doc *Document) hasTokenPrefix(prefix string, collator collate.Collator) bool {
	if doc == nil {
		return false
	}
	for token := range doc.TokenSet {
		if tokenHasPrefix(token, prefix, collator) {
			return true
		}
	}
	return false
}

func (col ColumnDocument) positionsFor(token string, collator collate.Collator) []int {
	if collator == nil {
		return col.Positions[token]
	}
	var positions []int
	for candidate, candidatePositions := range col.Positions {
		if collator.Compare(candidate, token) == 0 {
			positions = append(positions, candidatePositions...)
		}
	}
	if len(positions) > 1 {
		sort.Ints(positions)
	}
	return positions
}

func tokenHasPrefix(token, prefix string, collator collate.Collator) bool {
	if collator == nil {
		return stringsHasPrefix(token, prefix)
	}
	pattern := collator.Pattern()
	escaped := make([]byte, 0, len(prefix)+1)
	for i := 0; i < len(prefix); i++ {
		switch prefix[i] {
		case '\\', '%', '_':
			escaped = append(escaped, '\\')
		}
		escaped = append(escaped, prefix[i])
	}
	escaped = append(escaped, '%')
	pattern.Compile(string(escaped), '\\')
	return pattern.DoMatch(token)
}

func stringsHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
