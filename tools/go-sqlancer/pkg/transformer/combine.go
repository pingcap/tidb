// Copyright 2022 PingCAP, Inc.
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

package transformer

import "github.com/pingcap/tidb/parser/ast"

// CombinedTransformer is a transformer that combines two transformers.
type CombinedTransformer struct {
	t1 Transformer
	t2 Transformer
}

// Combine is a function that combines two transformers.
func Combine(t1, t2 Transformer) *CombinedTransformer {
	return &CombinedTransformer{t1: t1, t2: t2}
}

// Transform is a function that transforms a list of statements into a new list of statements.
func (h *CombinedTransformer) Transform(nodes []ast.ResultSetNode) []ast.ResultSetNode {
	return h.t2.Transform(h.t1.Transform(nodes))
}
