// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package optimizer

import "github.com/pingcap/tidb/ast"

// conditionNormalizer converts an condition expression into
// CNF(Conjunctive normal form).
// See https://en.wikipedia.org/wiki/Conjunctive_normal_form
// An boolean expression(usually a where condition) need to be converted
// to a series of AND conditions, so if a sub condition match an index,
// it can be pushed to that index to reduce cost.
// Ssub conditions that can not be pushed to index will be put into a Filter plan.
type conditionNormalizer struct {
}

func (b *conditionNormalizer) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (b *conditionNormalizer) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func normalizeCondition(node ast.Node) {

}
