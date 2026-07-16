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

package autoembed

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
)

// ConsumerDistanceFunction maps the complete set of functions that consume
// auto-embedding provenance to their ordinary vector-distance counterparts.
func ConsumerDistanceFunction(name string) (string, bool) {
	switch name {
	case ast.VecEmbedL1Distance:
		return ast.VecL1Distance, true
	case ast.VecEmbedL2Distance:
		return ast.VecL2Distance, true
	case ast.VecEmbedNegativeInnerProduct:
		return ast.VecNegativeInnerProduct, true
	case ast.VecEmbedCosineDistance:
		return ast.VecCosineDistance, true
	default:
		return "", false
	}
}

// ClassifyConsumerAST performs a consumer-only structural traversal. It is for
// ASTs materialized after the statement's normal Preprocess traversal, such as
// runtime view definitions and plan-digest EXPLAIN statements.
func ClassifyConsumerAST(node ast.Node) resolve.AutoEmbedConsumerPresence {
	if node == nil {
		return resolve.AutoEmbedConsumerUnknown
	}
	v := &consumerClassifier{}
	_, complete := node.Accept(v)
	if v.found {
		return resolve.AutoEmbedConsumerPresent
	}
	if !complete {
		return resolve.AutoEmbedConsumerUnknown
	}
	return resolve.AutoEmbedConsumerAbsent
}

// ClassifyAssignmentExprs classifies only ON DUPLICATE assignment right-hand
// sides. An empty list is reliably Absent; a nil assignment or expression is
// Unknown and therefore conservatively enables source snapshots.
func ClassifyAssignmentExprs(assignments []*ast.Assignment) resolve.AutoEmbedConsumerPresence {
	presence := resolve.AutoEmbedConsumerAbsent
	for _, assignment := range assignments {
		if assignment == nil || assignment.Expr == nil {
			presence = resolve.MergeAutoEmbedConsumerPresence(presence, resolve.AutoEmbedConsumerUnknown)
			continue
		}
		presence = resolve.MergeAutoEmbedConsumerPresence(presence, ClassifyConsumerAST(assignment.Expr))
		if presence == resolve.AutoEmbedConsumerPresent {
			return presence
		}
	}
	return presence
}

type consumerClassifier struct {
	found bool
}

func (v *consumerClassifier) Enter(node ast.Node) (ast.Node, bool) {
	if call, ok := node.(*ast.FuncCallExpr); ok {
		if _, found := ConsumerDistanceFunction(call.FnName.L); found {
			v.found = true
			return node, true
		}
	}
	return node, false
}

func (v *consumerClassifier) Leave(node ast.Node) (ast.Node, bool) {
	return node, !v.found
}
