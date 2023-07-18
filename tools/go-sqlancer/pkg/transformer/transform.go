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

import (
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
)

// Transformer is a function that transforms a list of statements into a new list of statements.
type Transformer interface {
	Transform([]ast.ResultSetNode) []ast.ResultSetNode
}

// Singleton is a function that transforms a list of statements into a new list of statements.
type Singleton func([]ast.ResultSetNode) []ast.ResultSetNode

// Transform is a function that transforms a list of statements into a new list of statements.
func (t Singleton) Transform(stmts []ast.ResultSetNode) []ast.ResultSetNode {
	return t(stmts)
}

// RandTransformer is a transformer that randomly applies a list of transformers.
func RandTransformer(transformers ...Transformer) Transformer {
	return transformers[util.Rd(len(transformers))]
}
