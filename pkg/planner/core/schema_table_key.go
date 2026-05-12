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

package core

import "github.com/pingcap/tidb/pkg/parser/ast"

// These keys represent SQL identifier identity. Keep all matching keys and
// key-derived diagnostics on the normalized CIStr.L fields.
type schemaTableKey struct {
	schema string
	table  string
}

func newSchemaTableKey(schema, table ast.CIStr) schemaTableKey {
	return schemaTableKey{schema: schema.L, table: table.L}
}

type tableAliasKey struct {
	schema    string
	name      string
	qualified bool
}

func newTableAliasKey(name ast.CIStr) tableAliasKey {
	return tableAliasKey{name: name.L}
}

func newQualifiedTableAliasKey(schema, name ast.CIStr) tableAliasKey {
	return tableAliasKey{schema: schema.L, name: name.L, qualified: true}
}

func (k tableAliasKey) displayName() ast.CIStr {
	if k.qualified {
		return ast.NewCIStr(k.schema + "." + k.name)
	}
	return ast.NewCIStr(k.name)
}
