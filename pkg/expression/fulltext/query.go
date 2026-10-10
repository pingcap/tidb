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
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// Query is an executable no-score boolean fulltext query.
type Query struct {
	root              queryNode
	matchCost         float64
	documentMatchCost float64
	matchesNothing    bool
	collator          collate.Collator
}

// CompileBooleanQuery parses and normalizes a BOOLEAN MODE query for local
// no-score MATCH ... AGAINST evaluation.
func CompileBooleanQuery(search string, config AnalyzerConfig) (*Query, error) {
	group, err := ParseBooleanQuery(search, config.ParserType)
	if err != nil {
		return nil, err
	}
	return CompileParsedBooleanQuery(group, config)
}

// CompileParsedBooleanQuery normalizes a parsed query without mutating its AST.
// Planning may reuse that AST for the TiFlash payload, independently of Tipb.
func CompileParsedBooleanQuery(group *matchagainst.BooleanGroup, config AnalyzerConfig) (*Query, error) {
	analyzer, err := GetAnalyzer(config)
	if err != nil {
		return nil, err
	}

	root, err := normalizeBooleanGroup(group, config, analyzer)
	if err != nil {
		return nil, err
	}
	work := estimateQueryNodeWork(root)
	query := &Query{
		root:              root,
		matchCost:         work.fixed,
		documentMatchCost: work.perDocument,
		matchesNothing:    queryNodeMatchesNothing(root),
		collator:          parserInfoFromConfig(config).collator,
	}
	return query, nil
}

// Match returns whether the document matches the no-score query.
func (q *Query) Match(doc *Document) bool {
	if q == nil || q.root == nil {
		return false
	}
	return q.root.match(doc, q.collator)
}

// MatchCost returns fixed query work such as term lookups and phrase setup.
// Work that scales with document size is reported by DocumentMatchCost.
func (q *Query) MatchCost() float64 {
	if q == nil {
		return 0
	}
	return q.matchCost
}

// DocumentMatchCost returns how many additional document-sized passes local
// matching performs after tokenization. Dense phrases use one KMP pass;
// prefixes scan the token set; sparse phrases may intersect one position list
// per analyzed query token.
func (q *Query) DocumentMatchCost() float64 {
	if q == nil {
		return 0
	}
	return q.documentMatchCost
}

// MatchesNothing reports whether normalization proved that no document can
// match, for example because a required term was removed by the analyzer.
func (q *Query) MatchesNothing() bool {
	return q == nil || q.matchesNothing
}

// ParseBooleanQuery selects the Boolean grammar for the configured parser.
func ParseBooleanQuery(search string, parserType model.FullTextParserType) (*matchagainst.BooleanGroup, error) {
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
	match(doc *Document, collator collate.Collator) bool
}

type neverNode struct{}

func (neverNode) match(*Document, collate.Collator) bool {
	return false
}

type termNode struct {
	token string
}

func (n termNode) match(doc *Document, collator collate.Collator) bool {
	return doc.hasToken(n.token, collator)
}

type prefixNode struct {
	prefix string
}

func (n prefixNode) match(doc *Document, collator collate.Collator) bool {
	return doc.hasTokenPrefix(n.prefix, collator)
}

type phraseNode struct {
	tokens  []string
	offsets []int
	failure []int
}

// verifiedPhraseNode mirrors InnoDB's candidate lookup followed by verification
// against the document, including words omitted from its fulltext index.
type verifiedPhraseNode struct {
	anchor   queryNode
	phrase   queryNode
	analyzer Analyzer
}

func (n verifiedPhraseNode) match(doc *Document, collator collate.Collator) bool {
	if !n.anchor.match(doc, collator) {
		return false
	}
	for _, column := range doc.Columns {
		raw, err := BuildDocument([]ColumnInput{{Text: column.SourceText}}, n.analyzer)
		if err == nil && n.phrase.match(raw, collator) {
			return true
		}
	}
	return false
}

func (n phraseNode) match(doc *Document, collator collate.Collator) bool {
	if doc == nil || len(n.tokens) == 0 || len(n.tokens) != len(n.offsets) {
		return false
	}
	for _, col := range doc.Columns {
		if n.isDense() {
			if n.matchDense(col, collator) {
				return true
			}
			continue
		}

		// Sparse offsets arise when the analyzer removes phrase terms. Start
		// from the rarest document token so a missing or selective term ends the
		// intersection before repeated common-token lists are scanned.
		anchor := -1
		for i, token := range n.tokens {
			positions := col.positionsFor(token, collator)
			if len(positions) == 0 {
				anchor = -1
				break
			}
			if anchor < 0 || len(positions) < len(col.positionsFor(n.tokens[anchor], collator)) {
				anchor = i
			}
		}
		if anchor < 0 {
			continue
		}
		anchorPositions := col.positionsFor(n.tokens[anchor], collator)
		candidates := make([]int, len(anchorPositions))
		for i, position := range anchorPositions {
			candidates[i] = position - n.offsets[anchor]
		}
		for i := range n.tokens {
			if i == anchor || len(candidates) == 0 {
				continue
			}
			candidates = intersectPhraseStarts(candidates, col.positionsFor(n.tokens[i], collator), n.offsets[i])
		}
		if len(candidates) > 0 {
			return true
		}
	}
	return false
}

func (n phraseNode) isDense() bool {
	if len(n.tokens) == 0 || len(n.tokens) != len(n.offsets) {
		return false
	}
	for i, offset := range n.offsets {
		if offset != i {
			return false
		}
	}
	return true
}

func (n phraseNode) matchDense(col ColumnDocument, collator collate.Collator) bool {
	if len(col.Tokens) < len(n.tokens) {
		return false
	}
	failure := n.failure
	if collator != nil {
		failure = buildPhraseFailureWithCollator(n.tokens, collator)
	} else if len(failure) != len(n.tokens) {
		failure = buildPhraseFailure(n.tokens)
	}
	matched := 0
	previousPosition := -1
	for _, token := range col.Tokens {
		if previousPosition >= 0 && token.Position != previousPosition+1 {
			matched = 0
		}
		previousPosition = token.Position
		for matched > 0 && !tokensEqual(token.Text, n.tokens[matched], collator) {
			matched = failure[matched-1]
		}
		if tokensEqual(token.Text, n.tokens[matched], collator) {
			matched++
			if matched == len(n.tokens) {
				return true
			}
		}
	}
	return false
}

func tokensEqual(lhs, rhs string, collator collate.Collator) bool {
	if collator == nil {
		return lhs == rhs
	}
	return collator.Compare(lhs, rhs) == 0
}

func buildPhraseFailureWithCollator(tokens []string, collator collate.Collator) []int {
	failure := make([]int, len(tokens))
	for i, matched := 1, 0; i < len(tokens); i++ {
		for matched > 0 && !tokensEqual(tokens[i], tokens[matched], collator) {
			matched = failure[matched-1]
		}
		if tokensEqual(tokens[i], tokens[matched], collator) {
			matched++
		}
		failure[i] = matched
	}
	return failure
}

func buildPhraseFailure(tokens []string) []int {
	failure := make([]int, len(tokens))
	for i, matched := 1, 0; i < len(tokens); i++ {
		for matched > 0 && tokens[i] != tokens[matched] {
			matched = failure[matched-1]
		}
		if tokens[i] == tokens[matched] {
			matched++
		}
		failure[i] = matched
	}
	return failure
}

func intersectPhraseStarts(starts, positions []int, offset int) []int {
	matched := starts[:0]
	for i, j := 0, 0; i < len(starts) && j < len(positions); {
		target := starts[i] + offset
		switch {
		case target < positions[j]:
			i++
		case target > positions[j]:
			j++
		default:
			matched = append(matched, starts[i])
			i++
			j++
		}
	}
	return matched
}

type groupNode struct {
	must    []queryNode
	should  []queryNode
	mustNot []queryNode
}

type queryWorkEstimate struct {
	fixed       float64
	perDocument float64
}

func (w *queryWorkEstimate) add(other queryWorkEstimate) {
	w.fixed += other.fixed
	w.perDocument += other.perDocument
}

func estimateQueryNodeWork(node queryNode) queryWorkEstimate {
	switch n := node.(type) {
	case nil:
		return queryWorkEstimate{}
	case neverNode:
		return queryWorkEstimate{fixed: 0.1}
	case termNode:
		return queryWorkEstimate{fixed: 1}
	case prefixNode:
		// Prefix matching scans the document's unique token set.
		return queryWorkEstimate{fixed: 1, perDocument: 1}
	case phraseNode:
		work := queryWorkEstimate{fixed: max(2, float64(len(n.tokens))*2)}
		if n.isDense() {
			work.perDocument = 1
		} else {
			work.perDocument = float64(len(n.tokens))
		}
		return work
	case groupNode:
		work := queryWorkEstimate{fixed: 1}
		for _, child := range n.must {
			work.add(estimateQueryNodeWork(child))
		}
		for _, child := range n.mustNot {
			work.add(estimateQueryNodeWork(child))
		}
		if len(n.must) == 0 {
			for _, child := range n.should {
				work.add(estimateQueryNodeWork(child))
			}
		}
		return work
	case verifiedPhraseNode:
		work := estimateQueryNodeWork(n.anchor)
		work.add(estimateQueryNodeWork(n.phrase))
		work.perDocument++ // Unfiltered analysis only after an anchor match.
		return work
	default:
		return queryWorkEstimate{fixed: 1}
	}
}

func queryNodeMatchesNothing(node queryNode) bool {
	switch n := node.(type) {
	case nil, neverNode:
		return true
	case termNode, prefixNode, phraseNode:
		return false
	case verifiedPhraseNode:
		return queryNodeMatchesNothing(n.anchor)
	case groupNode:
		for _, child := range n.must {
			if queryNodeMatchesNothing(child) {
				return true
			}
		}
		if len(n.must) > 0 {
			return false
		}
		if len(n.should) == 0 {
			return true
		}
		for _, child := range n.should {
			if !queryNodeMatchesNothing(child) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (n groupNode) match(doc *Document, collator collate.Collator) bool {
	for _, child := range n.must {
		if !child.match(doc, collator) {
			return false
		}
	}
	for _, child := range n.mustNot {
		if child.match(doc, collator) {
			return false
		}
	}
	if len(n.must) > 0 {
		return true
	}
	for _, child := range n.should {
		if child.match(doc, collator) {
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
		return normalizeBooleanTerm(x, clause.Modifier, config, analyzer)
	case *matchagainst.BooleanPhrase:
		return normalizeBooleanPhrase(x, config, analyzer)
	case *matchagainst.BooleanGroup:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE group expression")
	default:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE expression: %T", clause.Expr)
	}
}

func normalizeBooleanTerm(
	term *matchagainst.BooleanTerm,
	modifier matchagainst.BooleanModifier,
	config AnalyzerConfig,
	analyzer Analyzer,
) (queryNode, error) {
	if term == nil || term.Ignored {
		return nil, nil
	}
	if term.Wildcard {
		return normalizePrefixTerm(term.Text(), modifier, config, analyzer)
	}

	tokens, err := analyzer.Analyze(term.Text())
	if err != nil {
		return nil, err
	}
	switch config.ParserType {
	case model.FullTextParserTypeStandardV1:
		switch len(tokens) {
		case 0:
			// Optional/excluded filtered terms can be omitted. The caller turns
			// a filtered required term into neverNode, matching MySQL InnoDB.
			return nil, nil
		case 1:
			return termNode{token: tokens[0].Text}, nil
		default:
			children := make([]queryNode, 0, len(tokens))
			for _, token := range tokens {
				children = append(children, termNode{token: token.Text})
			}
			return combineBooleanTermNodes(children, modifier), nil
		}
	case model.FullTextParserTypeNgramV1:
		return normalizeAnalyzedPhrase(term.Text(), tokens, config)
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
	if config.ParserType == model.FullTextParserTypeNgramV1 {
		info := parserInfoFromConfig(config)
		seenIndexedWord := false
		for _, word := range PreserveUnderscoreTokenize(phrase.Text()) {
			// The Boolean plugin emits a short query word as a unigram. It
			// cannot occur in a fixed-size document ngram index. Do not drop
			// it and accidentally match the remaining phrase words.
			if charLen(word.Text) < config.NgramTokenSize {
				if seenIndexedWord || !info.containsNgramStopword(word.Text) {
					return neverNode{}, nil
				}
			} else {
				indexed, err := analyzer.Analyze(word.Text)
				if err != nil {
					return nil, err
				}
				seenIndexedWord = seenIndexedWord || len(indexed) > 0
			}
		}
	}
	return normalizeAnalyzedPhrase(phrase.Text(), tokens, config)
}

func normalizeAnalyzedPhrase(text string, tokens []Token, config AnalyzerConfig) (queryNode, error) {
	anchor := buildPhraseNode(tokens)
	if anchor == nil {
		return nil, nil
	}
	rawConfig := config
	rawConfig.InnodbFtEnableStopword = false
	rawConfig.InnodbFtMinTokenSize = 0
	rawConfig.InnodbFtMaxTokenSize = math.MaxInt
	rawAnalyzer, err := GetAnalyzer(rawConfig)
	if err != nil {
		return nil, err
	}
	rawTokens, err := rawAnalyzer.Analyze(text)
	if err != nil {
		return nil, err
	}
	// InnoDB drops leading filtered tokens from its original phrase vector,
	// but preserves all words after the first indexed token for verification.
	for len(rawTokens) > 0 && rawTokens[0].Position < tokens[0].Position {
		rawTokens = rawTokens[1:]
	}
	if len(rawTokens) == len(tokens) {
		return anchor, nil
	}
	return verifiedPhraseNode{anchor: anchor, phrase: buildPhraseNode(rawTokens), analyzer: rawAnalyzer}, nil
}

func normalizePrefixTerm(
	text string,
	modifier matchagainst.BooleanModifier,
	config AnalyzerConfig,
	analyzer Analyzer,
) (queryNode, error) {
	sourceTokens := PreserveUnderscoreTokenize(text)
	if len(sourceTokens) == 0 {
		return nil, nil
	}

	parserInfo := parserInfoFromConfig(config)
	switch config.ParserType {
	case model.FullTextParserTypeStandardV1:
		children := make([]queryNode, 0, len(sourceTokens))
		for _, sourceToken := range sourceTokens[:len(sourceTokens)-1] {
			tokens, err := analyzer.Analyze(sourceToken.Text)
			if err != nil {
				return nil, err
			}
			for _, token := range tokens {
				children = append(children, termNode{token: token.Text})
			}
		}

		// MySQL applies '*' to only the last word of a split term. The
		// wildcard also keeps that word when it is shorter than the normal
		// minimum token size or is a stopword.
		tail := sourceTokens[len(sourceTokens)-1]
		if charLen(tail.Text) <= parserInfo.innodbFtMaxTokenSize {
			prefix := tail.Text
			if parserInfo.collator == nil {
				prefix = strings.ToLower(prefix)
			}
			children = append(children, prefixNode{prefix: prefix})
		}
		return combineBooleanTermNodes(children, modifier), nil
	case model.FullTextParserTypeNgramV1:
		if len(sourceTokens) != 1 {
			return nil, nil
		}
		if charLen(sourceTokens[0].Text) < parserInfo.ngramTokenSize {
			prefix := sourceTokens[0].Text
			if parserInfo.collator == nil {
				prefix = strings.ToLower(prefix)
			}
			return prefixNode{prefix: prefix}, nil
		}
		tokens, err := analyzer.Analyze(text)
		if err != nil {
			return nil, err
		}
		return normalizeAnalyzedPhrase(text, tokens, config)
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type: %s", config.ParserType)
	}
}

func combineBooleanTermNodes(children []queryNode, modifier matchagainst.BooleanModifier) queryNode {
	switch len(children) {
	case 0:
		return nil
	case 1:
		return children[0]
	}
	if modifier == matchagainst.BooleanModifierMust {
		return groupNode{must: children}
	}
	return groupNode{should: children}
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
	if node.isDense() {
		node.failure = buildPhraseFailure(node.tokens)
	}
	return node
}
