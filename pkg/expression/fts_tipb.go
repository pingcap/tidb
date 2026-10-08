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

package expression

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
)

// BuildFTSBooleanQuery parses a BOOLEAN MODE search string and converts it to
// the protocol representation consumed by TiFlash. The parser type is part of
// the index metadata; callers must not use this helper to claim support for a
// parser which TiFlash cannot interpret.
func BuildFTSBooleanQuery(search string, parserType model.FullTextParserType) (*tipb.FTSBooleanQuery, error) {
	return BuildFTSBooleanQueryWithNgramTokenSize(search, parserType, 0)
}

// BuildFTSBooleanQueryWithNgramTokenSize parses a BOOLEAN MODE search string
// and attaches the analyzer configuration required by TiFlash. A zero NGRAM
// token size tells TiFlash to use its configured default.
func BuildFTSBooleanQueryWithNgramTokenSize(search string, parserType model.FullTextParserType, ngramTokenSize int) (*tipb.FTSBooleanQuery, error) {
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
	if containsFTSBooleanSubExpression(group) {
		return nil, fmt.Errorf("nested BOOLEAN MODE groups are not supported by TiFlash FTS pushdown")
	}
	query, err := buildFTSBooleanGroup(group)
	if err != nil {
		return nil, err
	}
	query.QueryTokenizer = string(parserType)
	if parserType == model.FullTextParserTypeNgramV1 && ngramTokenSize > 0 {
		query.NgramTokenSize = uint32(ngramTokenSize)
	}
	return query, nil
}

// BuildFTSBooleanQueryWithAnalyzerConfig carries the same analyzer settings
// used by TiDB's local evaluator to TiFlash. This extends only execution
// parity; it does not change BOOLEAN MODE query semantics.
func BuildFTSBooleanQueryWithAnalyzerConfig(search string, config fulltext.AnalyzerConfig) (*tipb.FTSBooleanQuery, error) {
	query, err := BuildFTSBooleanQueryWithNgramTokenSize(search, config.ParserType, config.NgramTokenSize)
	if err != nil {
		return nil, err
	}
	if config.ParserType == model.FullTextParserTypeStandardV1 {
		if config.InnodbFtMinTokenSize < 0 || config.InnodbFtMaxTokenSize <= 0 {
			return nil, fmt.Errorf("invalid STANDARD_V1 analyzer token-size range: min=%d max=%d",
				config.InnodbFtMinTokenSize, config.InnodbFtMaxTokenSize)
		}
		query.InnodbFtMinTokenSize = uint32(config.InnodbFtMinTokenSize)
		query.InnodbFtMaxTokenSize = uint32(config.InnodbFtMaxTokenSize)
		query.InnodbFtEnableStopword = config.InnodbFtEnableStopword
	}
	return query, nil
}

func containsFTSBooleanSubExpression(group *matchagainst.BooleanGroup) bool {
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

func buildFTSBooleanGroup(group *matchagainst.BooleanGroup) (*tipb.FTSBooleanQuery, error) {
	if group == nil {
		return nil, fmt.Errorf("nil fulltext boolean group")
	}
	query := &tipb.FTSBooleanQuery{}
	for _, clauses := range [][]matchagainst.BooleanClause{group.Must, group.Should, group.MustNot} {
		for _, clause := range clauses {
			node, err := buildFTSBooleanNode(clause)
			if err != nil {
				return nil, err
			}
			query.Nodes = append(query.Nodes, node)
		}
	}
	return query, nil
}

func buildFTSBooleanNode(clause matchagainst.BooleanClause) (*tipb.FTSBooleanNode, error) {
	switch clause.Modifier {
	case matchagainst.BooleanModifierNone, matchagainst.BooleanModifierMust, matchagainst.BooleanModifierMustNot:
	default:
		return nil, fmt.Errorf("unsupported BOOLEAN MODE score modifier")
	}
	node := &tipb.FTSBooleanNode{Occur: ftsBooleanOccur(clause.Modifier)}
	switch expr := clause.Expr.(type) {
	case *matchagainst.BooleanTerm:
		termType := tipb.FTSBooleanTermType_FTSBooleanTermWord
		if expr.Wildcard {
			termType = tipb.FTSBooleanTermType_FTSBooleanTermPrefix
		}
		node.Term = &tipb.FTSBooleanTerm{
			TermType: termType,
			Text:     expr.Text(),
		}
	case *matchagainst.BooleanPhrase:
		if expr.Distance != nil {
			return nil, fmt.Errorf("proximity BOOLEAN MODE phrases are not supported by TiFlash pushdown")
		}
		node.Term = &tipb.FTSBooleanTerm{
			TermType: tipb.FTSBooleanTermType_FTSBooleanTermPhrase,
			Text:     expr.Text(),
		}
	case *matchagainst.BooleanGroup:
		return nil, fmt.Errorf("nested BOOLEAN MODE groups are not supported by TiFlash FTS pushdown")
	default:
		return nil, fmt.Errorf("unsupported fulltext boolean expression %T", clause.Expr)
	}
	return node, nil
}

func ftsBooleanOccur(modifier matchagainst.BooleanModifier) tipb.FTSBooleanOccur {
	switch modifier {
	case matchagainst.BooleanModifierMust:
		return tipb.FTSBooleanOccur_FTSBooleanOccurMust
	case matchagainst.BooleanModifierMustNot:
		return tipb.FTSBooleanOccur_FTSBooleanOccurMustNot
	default:
		return tipb.FTSBooleanOccur_FTSBooleanOccurShould
	}
}
