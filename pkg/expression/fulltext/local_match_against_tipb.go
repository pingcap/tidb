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
const LocalMatchAgainstProtocolVersion uint32 = 1

// BuildLocalMatchAgainstBooleanQuery parses a BOOLEAN MODE search string and
// converts it to the protocol representation consumed by TiFlash. The parser
// type comes from FULLTEXT index configuration; callers must not claim support for a
// parser which TiFlash cannot interpret.
func BuildLocalMatchAgainstBooleanQuery(search string, parserType model.FullTextParserType) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	return BuildLocalMatchAgainstBooleanQueryWithNgramTokenSize(search, parserType, 0)
}

// BuildLocalMatchAgainstBooleanQueryWithNgramTokenSize parses a BOOLEAN MODE
// search string and attaches the analyzer configuration required by TiFlash.
// A zero NGRAM token size tells TiFlash to use its configured default.
func BuildLocalMatchAgainstBooleanQueryWithNgramTokenSize(search string, parserType model.FullTextParserType, ngramTokenSize int) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	var (
		group *matchagainst.BooleanGroup
		err   error
	)
	switch parserType {
	case model.FullTextParserTypeStandardV1:
		group, err = matchagainst.ParseStandardBooleanMode(search)
	case model.FullTextParserTypeNgramV1:
		group, err = matchagainst.ParseNgramBooleanMode(search)
	default:
		return nil, fmt.Errorf("unsupported fulltext parser type for TiFlash BOOLEAN MODE pushdown: %s", parserType)
	}
	if err != nil {
		return nil, err
	}
	if containsLocalMatchAgainstBooleanSubExpression(group) {
		return nil, fmt.Errorf("nested BOOLEAN MODE groups are not supported by TiFlash Local MATCH pushdown")
	}
	query, err := buildLocalMatchAgainstBooleanGroup(group)
	if err != nil {
		return nil, err
	}
	query.Version = LocalMatchAgainstProtocolVersion
	switch parserType {
	case model.FullTextParserTypeStandardV1:
		query.Parser = tipb.LocalMatchAgainstParser_LocalMatchAgainstParserStandard
	case model.FullTextParserTypeNgramV1:
		query.Parser = tipb.LocalMatchAgainstParser_LocalMatchAgainstParserNgram
	}
	if parserType == model.FullTextParserTypeNgramV1 && ngramTokenSize > 0 {
		query.NgramTokenSize = uint32(ngramTokenSize)
	}
	return query, nil
}

// BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig carries the same analyzer settings
// used by TiDB's Local MATCH evaluator to TiFlash. This extends only execution
// parity; it does not change BOOLEAN MODE query semantics.
func BuildLocalMatchAgainstBooleanQueryWithAnalyzerConfig(search string, config AnalyzerConfig) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	query, err := BuildLocalMatchAgainstBooleanQueryWithNgramTokenSize(search, config.ParserType, config.NgramTokenSize)
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

func containsLocalMatchAgainstBooleanSubExpression(group *matchagainst.BooleanGroup) bool {
	if group == nil {
		return false
	}
	for _, clauses := range [][]matchagainst.BooleanClause{group.Must, group.Should, group.MustNot} {
		for _, clause := range clauses {
			if _, ok := clause.Expr.(*matchagainst.BooleanGroup); ok {
				return true
			}
		}
	}
	return false
}

func buildLocalMatchAgainstBooleanGroup(group *matchagainst.BooleanGroup) (*tipb.LocalMatchAgainstBooleanQuery, error) {
	if group == nil {
		return nil, fmt.Errorf("nil Local MATCH Boolean group")
	}
	query := &tipb.LocalMatchAgainstBooleanQuery{}
	for _, clauses := range [][]matchagainst.BooleanClause{group.Must, group.Should, group.MustNot} {
		for _, clause := range clauses {
			node, err := buildLocalMatchAgainstBooleanNode(clause)
			if err != nil {
				return nil, err
			}
			query.Nodes = append(query.Nodes, node)
		}
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
