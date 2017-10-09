// Copyright 2017 PingCAP, Inc.
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

package cache

import (
	"github.com/pingcap/tidb/ast"
)

// Value is the interface that every value in LRU Cache should implement.
type Value interface {
}

// SQLCacheValue stores the cached Statement and StmtNode.
type SQLCacheValue struct {
	Stmt ast.Statement
	Ast  ast.StmtNode
}

// NewSQLCacheValue creates a SQLCacheValue.
func NewSQLCacheValue(stmt ast.Statement, ast ast.StmtNode) *SQLCacheValue {
	return &SQLCacheValue{
		Stmt: stmt,
		Ast:  ast,
	}
}
