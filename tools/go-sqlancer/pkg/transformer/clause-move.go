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

var (
	// TODO: implement where-to-on Transformer
	where2On Singleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
		return nodes
	}

	// TODO: implement on-to-where Transformer
	on2Where Singleton = func(nodes []ast.ResultSetNode) []ast.ResultSetNode {
		return nodes
	}
)
