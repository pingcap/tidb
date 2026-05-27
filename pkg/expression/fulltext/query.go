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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// Query is an executable no-score boolean fulltext query.
type Query struct {
	root queryNode
}

// CompileBooleanQuery parses and normalizes a BOOLEAN MODE query for local
// no-score MATCH ... AGAINST evaluation.
func CompileBooleanQuery(search string, config AnalyzerConfig) (*Query, error) {
	analyzer, err := GetAnalyzer(config)
	if err != nil {
		return nil, err
	}

	group, err := parseBooleanQuery(search, config.ParserType)
	if err != nil {
		return nil, err
	}
	root, err := normalizeBooleanGroup(group, config, analyzer)
	if err != nil {
		return nil, err
	}
	return &Query{root: root}, nil
}

// Match returns whether the document matches the no-score query.
func (q *Query) Match(doc *Document) bool {
	if q == nil || q.root == nil {
		return false
	}
	return q.root.match(doc)
}

func parseBooleanQuery(search string, parserType model.FullTextParserType) (*matchagainst.BooleanGroup, error) {
	switch parserType {
	case model.FullTextParserTypeStandardV1:
		return matchagainst.ParseStandardBooleanMode(search)
	case model.FullTextParserTypeNgramV1:
		return matchagainst.ParseNgramBooleanMode(search)
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type: %s", parserType)
	}
}

type queryNode interface {
	match(doc *Document) bool
}

type neverNode struct{}

func (neverNode) match(*Document) bool {
	return false
}

type termNode struct {
	token string
}

func (n termNode) match(doc *Document) bool {
	return doc.hasToken(n.token)
}

type prefixNode struct {
	prefix string
}

func (n prefixNode) match(doc *Document) bool {
	return doc.hasTokenPrefix(n.prefix)
}

type phraseNode struct {
	tokens  []string
	offsets []int
}

func (n phraseNode) match(doc *Document) bool {
	if doc == nil || len(n.tokens) == 0 || len(n.tokens) != len(n.offsets) {
		return false
	}
	firstToken := n.tokens[0]
	for _, col := range doc.Columns {
		startPositions := col.Positions[firstToken]
		for _, start := range startPositions {
			if n.matchesAt(col, start) {
				return true
			}
		}
	}
	return false
}

func (n phraseNode) matchesAt(col ColumnDocument, start int) bool {
	for i := 1; i < len(n.tokens); i++ {
		if !positionsContain(col.Positions[n.tokens[i]], start+n.offsets[i]) {
			return false
		}
	}
	return true
}

func positionsContain(positions []int, target int) bool {
	for _, pos := range positions {
		if pos == target {
			return true
		}
	}
	return false
}

type groupNode struct {
	must    []queryNode
	should  []queryNode
	mustNot []queryNode
}

func (n groupNode) match(doc *Document) bool {
	for _, child := range n.must {
		if !child.match(doc) {
			return false
		}
	}
	for _, child := range n.mustNot {
		if child.match(doc) {
			return false
		}
	}
	if len(n.must) > 0 {
		return true
	}
	for _, child := range n.should {
		if child.match(doc) {
			return true
		}
	}
	return false
}

func normalizeBooleanGroup(group *matchagainst.BooleanGroup, config AnalyzerConfig, analyzer Analyzer) (queryNode, error) {
	if group == nil {
		return nil, fmt.Errorf("invalid nil BOOLEAN MODE query")
	}

	node := groupNode{}
	for _, clause := range group.Must {
		child, err := normalizeBooleanClause(clause, config, analyzer)
		if err != nil {
			return nil, err
		}
		if child == nil {
			child = neverNode{}
		}
		node.must = append(node.must, child)
	}
	for _, clause := range group.MustNot {
		child, err := normalizeBooleanClause(clause, config, analyzer)
		if err != nil {
			return nil, err
		}
		if child != nil {
			node.mustNot = append(node.mustNot, child)
		}
	}
	for _, clause := range group.Should {
		child, err := normalizeBooleanClause(clause, config, analyzer)
		if err != nil {
			return nil, err
		}
		if child != nil {
			node.should = append(node.should, child)
		}
	}
	return node, nil
}

func normalizeBooleanClause(clause matchagainst.BooleanClause, config AnalyzerConfig, analyzer Analyzer) (queryNode, error) {
	switch clause.Modifier {
	case matchagainst.BooleanModifierNone, matchagainst.BooleanModifierMust, matchagainst.BooleanModifierMustNot:
	default:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE score modifier")
	}

	switch x := clause.Expr.(type) {
	case *matchagainst.BooleanTerm:
		return normalizeBooleanTerm(x, config, analyzer)
	case *matchagainst.BooleanPhrase:
		return normalizeBooleanPhrase(x, config, analyzer)
	case *matchagainst.BooleanGroup:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE group expression")
	default:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE expression: %T", clause.Expr)
	}
}

func normalizeBooleanTerm(term *matchagainst.BooleanTerm, config AnalyzerConfig, analyzer Analyzer) (queryNode, error) {
	if term == nil || term.Ignored {
		return nil, nil
	}
	if term.Wildcard {
		return normalizePrefixTerm(term.Text(), config), nil
	}

	tokens, err := analyzer.Analyze(term.Text())
	if err != nil {
		return nil, err
	}
	switch config.ParserType {
	case model.FullTextParserTypeStandardV1:
		if len(tokens) != 1 {
			return nil, nil
		}
		return termNode{token: tokens[0].Text}, nil
	case model.FullTextParserTypeNgramV1:
		return buildPhraseNode(tokens), nil
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type: %s", config.ParserType)
	}
}

func normalizeBooleanPhrase(phrase *matchagainst.BooleanPhrase, config AnalyzerConfig, analyzer Analyzer) (queryNode, error) {
	if phrase == nil {
		return nil, nil
	}
	if phrase.Distance != nil {
		return nil, fmt.Errorf("unsupported BOOLEAN MODE phrase proximity")
	}
	tokens, err := analyzer.Analyze(phrase.Text())
	if err != nil {
		return nil, err
	}
	return buildPhraseNode(tokens), nil
}

func normalizePrefixTerm(text string, config AnalyzerConfig) queryNode {
	sourceTokens := PreserveUnderscoreTokenize(text)
	if len(sourceTokens) != 1 {
		return nil
	}

	parserInfo := parserInfoFromConfig(config)
	switch config.ParserType {
	case model.FullTextParserTypeStandardV1:
		token := sourceTokens[0]
		if charLen(token.Text) > parserInfo.innodbFtMaxTokenSize {
			return nil
		}
		return prefixNode{prefix: strings.ToLower(token.Text)}
	case model.FullTextParserTypeNgramV1:
		tokens := ngramFilter(sourceTokens, parserInfo.ngramTokenSize, parserInfo.ngramTokenSize)
		tokens = lowerFilter(tokens)
		if len(tokens) != 1 {
			return nil
		}
		return prefixNode{prefix: tokens[0].Text}
	default:
		return nil
	}
}

func buildPhraseNode(tokens []Token) queryNode {
	if len(tokens) == 0 {
		return nil
	}
	if len(tokens) == 1 {
		return termNode{token: tokens[0].Text}
	}

	firstPosition := tokens[0].Position
	node := phraseNode{
		tokens:  make([]string, 0, len(tokens)),
		offsets: make([]int, 0, len(tokens)),
	}
	for _, token := range tokens {
		node.tokens = append(node.tokens, token.Text)
		node.offsets = append(node.offsets, token.Position-firstPosition)
	}
	return node
}
