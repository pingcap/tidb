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

func (doc *Document) hasToken(token string) bool {
	if doc == nil {
		return false
	}
	_, ok := doc.TokenSet[token]
	return ok
}

func (doc *Document) hasTokenPrefix(prefix string) bool {
	if doc == nil {
		return false
	}
	for token := range doc.TokenSet {
		if stringsHasPrefix(token, prefix) {
			return true
		}
	}
	return false
}

func stringsHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
