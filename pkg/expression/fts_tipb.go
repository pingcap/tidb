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

	"github.com/pingcap/tidb/pkg/expression/matchagainst"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tipb/go-tipb"
)

// BuildFTSBooleanQuery parses a BOOLEAN MODE search string and converts it to
// the protocol representation consumed by TiFlash. The parser type is part of
// the index metadata; callers must not use this helper to claim support for a
// parser which TiFlash cannot interpret.
func BuildFTSBooleanQuery(search string, parserType model.FullTextParserType) (*tipb.FTSBooleanQuery, error) {
	if parserType != model.FullTextParserTypeStandardV1 {
		return nil, fmt.Errorf("unsupported fulltext parser type for TiFlash BOOLEAN MODE pushdown: %s", parserType)
	}
	group, err := matchagainst.ParseStandardBooleanMode(search)
	if err != nil {
		return nil, err
	}
	return buildFTSBooleanGroup(group)
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
	node := &tipb.FTSBooleanNode{
		Occur:    ftsBooleanOccur(clause.Modifier),
		Modifier: ftsBooleanModifier(clause.Modifier),
	}
	switch expr := clause.Expr.(type) {
	case *matchagainst.BooleanTerm:
		termType := tipb.FTSBooleanTermType_FTSBooleanTermWord
		if expr.Wildcard {
			termType = tipb.FTSBooleanTermType_FTSBooleanTermPrefix
		}
		node.Node = &tipb.FTSBooleanNode_Term{Term: &tipb.FTSBooleanTerm{
			TermType: termType,
			Text:     expr.Text(),
		}}
	case *matchagainst.BooleanPhrase:
		term := &tipb.FTSBooleanTerm{
			TermType: tipb.FTSBooleanTermType_FTSBooleanTermPhrase,
			Text:     expr.Text(),
		}
		if expr.Distance != nil {
			if *expr.Distance < 0 {
				return nil, fmt.Errorf("negative fulltext phrase distance: %d", *expr.Distance)
			}
			term.PhraseDistance = uint32(*expr.Distance)
		}
		node.Node = &tipb.FTSBooleanNode_Term{Term: term}
	case *matchagainst.BooleanGroup:
		subQuery, err := buildFTSBooleanGroup(expr)
		if err != nil {
			return nil, err
		}
		node.Node = &tipb.FTSBooleanNode_SubExpression{SubExpression: subQuery}
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

func ftsBooleanModifier(modifier matchagainst.BooleanModifier) tipb.FTSBooleanModifier {
	switch modifier {
	case matchagainst.BooleanModifierBoost:
		return tipb.FTSBooleanModifier_FTSBooleanModifierBoost
	case matchagainst.BooleanModifierDeBoost:
		return tipb.FTSBooleanModifier_FTSBooleanModifierDeBoost
	case matchagainst.BooleanModifierNegate:
		return tipb.FTSBooleanModifier_FTSBooleanModifierNegate
	default:
		return tipb.FTSBooleanModifier_FTSBooleanModifierNone
	}
}
