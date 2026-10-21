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

	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
)

// LocalMatchAgainstProtocolVersion versions the Local MATCH semantics carried
// by the Tipb query, including the built-in stopword set. TiFlash must implement
// and be deployed with a version before TiDB emits it. Never change the meaning
// of an existing version; reject unknown versions and add a new version instead.
// Version 2 returns zero for NULL searches and gives NGRAM document scanning
// its own multibyte-character rule; STANDARD and the NGRAM query lexer use
// BMP word characters. Filtered phrases are verified against unfiltered text.
const LocalMatchAgainstProtocolVersion uint32 = 2

func buildParsedLocalMatchAgainstBooleanQuery(group *matchagainst.BooleanGroup, parserType model.FullTextParserType, ngramTokenSize int) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	if group == nil {
		return nil, fmt.Errorf("nil Local MATCH Boolean group")
	}
	clausesByOccur := [...][]matchagainst.BooleanClause{group.Must, group.Should, group.MustNot}
	// Preserve capability-error precedence: any nested group wins over a
	// split STANDARD prefix, and both win over node-conversion errors. Record
	// split prefixes while checking groups instead of scanning the AST again.
	hasSplitPrefix := false
	for _, clauses := range clausesByOccur {
		for _, clause := range clauses {
			switch expr := clause.Expr.(type) {
			case *matchagainst.BooleanGroup:
				return nil, fmt.Errorf("nested BOOLEAN MODE groups are not supported by TiFlash Local MATCH pushdown")
			case *matchagainst.BooleanTerm:
				if !hasSplitPrefix && parserType == model.FullTextParserTypeStandardV1 && expr.Wildcard {
					hasSplitPrefix = len(PreserveUnderscoreTokenize(expr.Text())) > 1
				}
			}
		}
	}
	if hasSplitPrefix {
		// TiDB treats earlier tokens as exact terms and only the last as a
		// prefix. TiFlash currently expects a single STANDARD prefix token.
		return nil, fmt.Errorf("split STANDARD prefix terms require TiDB Local MATCH evaluation")
	}
	query := &tipb.LocalMatchAgainstBooleanQuery{Version: LocalMatchAgainstProtocolVersion}
	for _, clauses := range clausesByOccur {
		for _, clause := range clauses {
			node, err := buildLocalMatchAgainstBooleanNode(clause)
			if err != nil {
				return nil, err
			}
			query.Nodes = append(query.Nodes, node)
		}
	}
	switch parserType {
	case model.FullTextParserTypeStandardV1:
		query.Parser = tipb.LocalMatchAgainstParser_LocalMatchAgainstParserStandard
	case model.FullTextParserTypeNgramV1:
		query.Parser = tipb.LocalMatchAgainstParser_LocalMatchAgainstParserNgram
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type: %s", parserType)
	}
	if parserType == model.FullTextParserTypeNgramV1 && ngramTokenSize > 0 {
		query.NgramTokenSize = uint32(ngramTokenSize)
	}
	return query, nil
}

// AnalyzerConfigFromLocalMatchAgainstBooleanQuery restores the wire analyzer
// settings for Go coprocessor/mock-MPP evaluation. Never read session sysvars
// here: they may differ from the session that constructed the request.
func AnalyzerConfigFromLocalMatchAgainstBooleanQuery(query *tipb.LocalMatchAgainstBooleanQuery, collation string) (AnalyzerConfig, error) {
	if query.GetVersion() != LocalMatchAgainstProtocolVersion {
		return AnalyzerConfig{}, fmt.Errorf("invalid Local MATCH protocol version %d", query.GetVersion())
	}
	config := AnalyzerConfig{
		Collation:            collation,
		StopwordCollation:    query.GetStopwordCollation(),
		InnodbFtMinTokenSize: int(query.GetInnodbFtMinTokenSize()),
		InnodbFtMaxTokenSize: int(query.GetInnodbFtMaxTokenSize()),
		NgramTokenSize:       int(query.GetNgramTokenSize()),
	}
	switch query.GetParser() {
	case tipb.LocalMatchAgainstParser_LocalMatchAgainstParserStandard:
		config.ParserType = model.FullTextParserTypeStandardV1
	case tipb.LocalMatchAgainstParser_LocalMatchAgainstParserNgram:
		config.ParserType = model.FullTextParserTypeNgramV1
	default:
		return AnalyzerConfig{}, fmt.Errorf("invalid Local MATCH parser %d", query.GetParser())
	}
	switch query.GetStopwordMode() {
	case tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled:
	case tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeBuiltin:
		config.InnodbFtEnableStopword = true
	default:
		return AnalyzerConfig{}, fmt.Errorf("unsupported Local MATCH stopword mode %d", query.GetStopwordMode())
	}
	// Version 2 retains these defaults for omitted analyzer fields, matching
	// TiFlash. In particular, an explicit min=0 with max>0 is not a default.
	if config.InnodbFtMinTokenSize == 0 && config.InnodbFtMaxTokenSize == 0 {
		config.InnodbFtMinTokenSize, config.InnodbFtMaxTokenSize = 3, 84
	}
	if config.NgramTokenSize == 0 {
		config.NgramTokenSize = 2
	}
	return config, nil
}

// BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig carries the same analyzer settings
// used by TiDB's Local MATCH evaluator to TiFlash. This extends only execution
// parity; it does not change BOOLEAN MODE query semantics.
func BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig(search string, config AnalyzerConfig) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	group, err := ParseBooleanQuery(search, config.ParserType)
	if err != nil {
		return nil, err
	}
	return BuildParsedLocalMatchAgainstBooleanQuery(group, config)
}

// BuildParsedLocalMatchAgainstBooleanQuery serializes an already parsed AST.
// It never modifies the AST: local compilation and capability checks may reuse it.
func BuildParsedLocalMatchAgainstBooleanQuery(group *matchagainst.BooleanGroup, config AnalyzerConfig) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	query, err := buildParsedLocalMatchAgainstBooleanQuery(group, config.ParserType, config.NgramTokenSize)
	if err != nil {
		return nil, err
	}
	// The flag applies to both parsers. NGRAM stopword filtering uses the same
	// built-in InnoDB stopword list as STANDARD_V1, with parser-specific rules.
	if config.InnodbFtEnableStopword {
		query.StopwordMode = tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeBuiltin
	} else {
		query.StopwordMode = tipb.LocalMatchAgainstStopwordMode_LocalMatchAgainstStopwordModeDisabled
	}
	query.StopwordCollation = config.StopwordCollation
	if config.ParserType == model.FullTextParserTypeStandardV1 {
		if config.InnodbFtMinTokenSize < 0 || config.InnodbFtMaxTokenSize <= 0 {
			return nil, fmt.Errorf("invalid STANDARD_V1 analyzer token-size range: min=%d max=%d",
				config.InnodbFtMinTokenSize, config.InnodbFtMaxTokenSize)
		}
		query.InnodbFtMinTokenSize = uint32(config.InnodbFtMinTokenSize)
		query.InnodbFtMaxTokenSize = uint32(config.InnodbFtMaxTokenSize)
	}
	return query, nil
}

func buildLocalMatchAgainstBooleanNode(clause matchagainst.BooleanClause) (*tipb.LocalMatchAgainstBooleanNode, error) {
	switch clause.Modifier {
	case matchagainst.BooleanModifierNone, matchagainst.BooleanModifierMust, matchagainst.BooleanModifierMustNot:
	default:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE score modifier")
	}
	node := &tipb.LocalMatchAgainstBooleanNode{Occur: localMatchAgainstBooleanOccur(clause.Modifier)}
	switch expr := clause.Expr.(type) {
	case *matchagainst.BooleanTerm:
		termType := tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermWord
		if expr.Wildcard {
			termType = tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermPrefix
		}
		node.TermType = termType
		node.Text = expr.Text()
	case *matchagainst.BooleanPhrase:
		if expr.Distance != nil {
			return nil, fmt.Errorf("proximity BOOLEAN MODE phrases are not supported by TiFlash pushdown")
		}
		node.TermType = tipb.LocalMatchAgainstBooleanTermType_LocalMatchAgainstBooleanTermPhrase
		node.Text = expr.Text()
	case *matchagainst.BooleanGroup:
		return nil, fmt.Errorf("nested BOOLEAN MODE groups are not supported by TiFlash Local MATCH pushdown")
	default:
		return nil, fmt.Errorf("unsupported Local MATCH Boolean expression %T", clause.Expr)
	}
	return node, nil
}

func localMatchAgainstBooleanOccur(modifier matchagainst.BooleanModifier) tipb.LocalMatchAgainstBooleanOccur {
	switch modifier {
	case matchagainst.BooleanModifierMust:
		return tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMust
	case matchagainst.BooleanModifierMustNot:
		return tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurMustNot
	default:
		return tipb.LocalMatchAgainstBooleanOccur_LocalMatchAgainstBooleanOccurShould
	}
}
